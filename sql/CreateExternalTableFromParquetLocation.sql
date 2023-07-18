/*

PRE-REQUISITES
==============
This stored procedure uses the SQL Managed Instance's Managed Identity to access the Storage Account location.
Ensure the Azure SQL Managed Instance Managed Identity has read access to whatever Parquet locations you specify
For example, assign the Storage Blob Data Reader to the SQL Managed Instance's Managed Identity

*/

-- Drop dbo.usp_PrintMax if exists 
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND OBJECT_ID = OBJECT_ID('dbo.usp_PrintMax'))
    DROP PROCEDURE [dbo].[usp_PrintMax];
GO

-- This helper proc will allow our main proc to generate debug output without worrying about truncation of long text strings
CREATE PROCEDURE [dbo].[usp_PrintMax] 
    @message NVARCHAR(MAX)
AS 
-- Credit: https://stackoverflow.com/questions/7850477/how-to-print-varcharmax-using-print-statement
BEGIN
    DECLARE @severity INT = 0,
            @start_pos INT = 1,
            @end_pos INT,
            @length INT = LEN(@message),
            @sub_message NVARCHAR(MAX),
            @cleaned_message NVARCHAR(MAX) = REPLACE(@message,'%','%%');
 
    WHILE (@start_pos <= @length)
    BEGIN
        SET @end_pos = CHARINDEX(CHAR(13) + CHAR(10), @cleaned_message + CHAR(13) + CHAR(10), @start_pos);
        SET @sub_message = SUBSTRING(@cleaned_message, @start_pos, @end_pos - @start_pos);
        EXEC sp_executesql N'RAISERROR(@msg, @severity, 10) WITH NOWAIT;', N'@msg NVARCHAR(MAX), @severity INT', @sub_message, @severity;
        SELECT @start_pos = @end_pos + 2, @severity = 0; 
    END

    RETURN 0;
END;
GO


-- Drop dbo.usp_CreateExternalTableFromSourceTable if it exists
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND [object_id] = OBJECT_ID('dbo.usp_CreateExternalTableFromParquetFilePath'))
    DROP PROCEDURE [dbo].[usp_CreateExternalTableFromParquetFilePath];
GO

-- Create Stored procedure to generate an external table which uses Polybase to retrieve Parquet data from an external storage account
-- This proc will infer the schema of the Parquet file(s) in order to generate the create table statement.
--
-- Note that the Parquet file path should either be a folder (with a trailing '/' character), or reference one or more Parquet files
-- In the case where a filename is provided, this will be replaced by a wildcard match for Parquet files - i.e. *.Parquet.
--
-- Parquet file paths support folders/sub-folders and partitions.  If you specify one or more partition colums using <partition field>=* in the file path
-- then the table will include additional derived columns for your partition fields.
-- 
-- For example if the file path is: /year=*/month=*/*.parquet, then two additional derived fields will be added for 'year' and 'month' with the values
-- derived from the partition values using the filepath() function.  
--
-- In order to derive the schema of the Parquet file(s), we create a temporary view using the OPENROWSET automatic schema inference, and then
-- retrieve the schema from the INFORMATION_SCHEMA.COLUMNS metadata, before removing the view and creating the external table
--
CREATE PROCEDURE [dbo].[usp_CreateExternalTableFromParquetFilePath]
    @storage_account            VARCHAR(100),
    @container                  VARCHAR(100),
    @path                       VARCHAR(1000),
    @external_table_name        SYSNAME,
    @external_table_schema      SYSNAME         = 'dbo',
    @create_external_table_sql  NVARCHAR(MAX)   = NULL OUTPUT,
    @drop_existing              BIT             = 1,
    @debug_only                 BIT             = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql_cmd                    NVARCHAR(MAX),
            @master_key_password        NVARCHAR(50),
            @data_source_name           VARCHAR(350)    = @storage_account + '-' + @container,
            @temp_view_name             SYSNAME         = 'vw_' + @external_table_name + '__tmp',
            @filename                   VARCHAR(255),
            @parquet_location           VARCHAR(1024),
            @partition_view_columns     NVARCHAR(MAX),
            @partition_table_columns    NVARCHAR(MAX),
            @columns_sql                NVARCHAR(MAX);

    -- Generate a database master key if it doesn't exist; use a random strong password
    IF NOT EXISTS(SELECT 1 FROM sys.symmetric_keys WHERE name LIKE '%DatabaseMasterKey%')
    BEGIN
        SELECT @master_key_password = CAST(N'' AS XML).value('xs:base64Binary(xs:hexBinary(sql:column("bin")))', 'VARCHAR(MAX)') FROM (SELECT CONVERT(VARBINARY(MAX), CAST(CRYPT_GEN_RANDOM(17) AS NVARCHAR(50))) AS bin) AS x;
        SET @sql_cmd = N'CREATE MASTER KEY ENCRYPTION BY PASSWORD = ''' + @master_key_password + '''';
        EXEC sp_executesql @sql_cmd;
    END

    -- Generate a Managed Identity credential for the storage account - use the same name as the data source (which reflects the storage account/container being referenced)
    SET @sql_cmd = N'
    IF NOT EXISTS(SELECT 1 FROM sys.database_scoped_credentials WHERE [name] = ''' + @data_source_name + ''')
        CREATE DATABASE SCOPED CREDENTIAL [' + @data_source_name + ']
        WITH IDENTITY = ''Managed Identity''; 
    ';
    EXEC sp_executesql @sql_cmd;

    -- Create an external data source based on root path of the storage container, referencing the managed identity credential
    SET @sql_cmd = N'
    IF NOT EXISTS(SELECT 1 FROM sys.external_data_sources WHERE [name] = ''' + @data_source_name + ''')
        CREATE EXTERNAL DATA SOURCE [' + @data_source_name + ']
        WITH (
            LOCATION = ''abs://' + @container + '@' + @storage_account + '.blob.core.windows.net/'',
            CREDENTIAL = [' + @data_source_name + ']
        );
    ';
    EXEC sp_executesql @sql_cmd;

    -- Create External File Format for PARQUET with Snappy compression if this doesn't exist already
    IF NOT EXISTS(SELECT 1 FROM sys.external_file_formats WHERE [name] = 'ParquetFileFormat')
        CREATE EXTERNAL FILE FORMAT ParquetFileFormat
        WITH (
            FORMAT_TYPE = PARQUET,
            DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
        );

    -- Now we have the data source/credentials, we need to create a temporary view to infer the schema.
    -- To do this, we need to make sure the Parquet location is wildcarded if required and also that
    -- we have a specification for the Partition fields (based on any wildcard sub-folder paths)
    SELECT @filename = RIGHT(@path, CHARINDEX('/', REVERSE(@path)) - 1);
    SELECT @parquet_location = LEFT(@path, 1 + LEN(@path) - CHARINDEX('/', REVERSE(@path)))

    -- Replace any filename part of the path with wildcard search of parquet
    IF LEN(@filename) > 0
        SELECT @parquet_location = @parquet_location + '*.parquet'

    -- Get Partition Columns SQL if any of the sub-folders are partitioned
    ;WITH PartitionColumns AS (
        SELECT
            REPLACE([value],'=*','') AS [PartitionColumn],
            ROW_NUMBER() OVER (ORDER BY [ordinal] ASC) AS [PartitionIndex]
        FROM 
            STRING_SPLIT(@path, '/', 1)
        WHERE [value] LIKE '%=*'
    )
    SELECT
        @partition_view_columns = ISNULL(
        ',' + CHAR(13) + CHAR(10) + 
        STRING_AGG(
            CONVERT(NVARCHAR(MAX),CHAR(9) + CHAR(9) + 'filerows.filepath(' + CAST([PartitionIndex] AS VARCHAR(10)) + ') AS [__tmp_partioned_by_' + [PartitionColumn] + ']'), ',' + CHAR(13) + CHAR(10)
        ) WITHIN GROUP (ORDER BY [PartitionIndex]),
        ''),
        @partition_table_columns = ISNULL(
        ',' + CHAR(13) + CHAR(10) + 
        STRING_AGG(
            CONVERT(NVARCHAR(MAX),CHAR(9) + '[Partition_' + [PartitionColumn] + '] AS filepath(' + CAST([PartitionIndex] AS VARCHAR(10)) + ')'), ',' + CHAR(13) + CHAR(10)
        ) WITHIN GROUP (ORDER BY [PartitionIndex]),
        '')
    FROM PartitionColumns;

    -- Drop temp view if it exists
    SET @sql_cmd = N'DROP VIEW IF EXISTS [' + @external_table_schema + '].[' + @temp_view_name + '];';
    EXEC sp_executesql @sql_cmd;

    -- Now create temp view
    SET @sql_cmd = N'
    CREATE VIEW [' + @external_table_schema + '].[' + @temp_view_name + '] AS 
    SELECT  
	    *' + @partition_view_columns  + '
    FROM OPENROWSET(
        BULK ''' + @parquet_location + ''',
        DATA_SOURCE = ''' + @data_source_name + ''',
        FORMAT = ''parquet''
    ) AS filerows;
    ';
    EXEC sp_executesql @sql_cmd;

    -- Now the view is created, we can get the inferred schema from INFORMATION_SCHEMA.COLUMNS
    -- As a starting point, get the column definitions SQL
    SELECT 
        @columns_sql = STRING_AGG(
            CONVERT(NVARCHAR(MAX),
            CHAR(9) + '[' + COLUMN_NAME + '] ' + 
            UPPER(DATA_TYPE) + ISNULL('(' + CASE WHEN CHARACTER_MAXIMUM_LENGTH < 0 THEN 'MAX' ELSE CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR(10)) END + ')','') + ' ' + 
            CASE WHEN IS_NULLABLE = 'YES' THEN 'NULL' ELSE 'NOT NULL' END),
            ',' + CHAR(13) + CHAR(10)
        ) WITHIN GROUP (ORDER BY ORDINAL_POSITION) + 
        @partition_table_columns
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
        TABLE_SCHEMA = @external_table_schema
    AND TABLE_NAME = @temp_view_name
    AND COLUMN_NAME NOT LIKE '__tmp_partioned_by_%';

    -- Now finally generate the CREATE EXTERNAL TABLE SQL Statement
    SET @create_external_table_sql = N'';

    IF @drop_existing = 1
        SET @create_external_table_sql = @create_external_table_sql + N'
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [object_id] = OBJECT_ID(''' + @external_table_schema + '.' + @external_table_name + '''))
    DROP EXTERNAL TABLE [' + @external_table_schema + '].[' + @external_table_name + '];
';

    SET @create_external_table_sql = @create_external_table_sql + N'
CREATE EXTERNAL TABLE [' + @external_table_schema + '].[' + @external_table_name + '] (
' + @columns_sql + '
)
WITH (
    LOCATION = ''' + @parquet_location + ''',
    DATA_SOURCE = [' + @data_source_name + '],
    FILE_FORMAT = [ParquetFileFormat],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0 
);
';

    -- Now drop the temporary view since this is no longer needed
    SET @sql_cmd = N'DROP VIEW IF EXISTS [' + @external_table_schema + '].[' + @temp_view_name + '];';
    EXEC sp_executesql @sql_cmd;

    -- Either print out or run the generated create external table script
    IF @debug_only = 0
        EXEC sp_executesql @create_external_table_sql;
    ELSE
        EXEC dbo.usp_PrintMax @create_external_table_sql;

END;
GO

-- Examples:
-- EXEC [dbo].[usp_CreateExternalTableFromParquetFilePath] 'saawbblobdevuks2', 'cetas-demo', 'AdventureWorksDW2019/dbo/FactInternetSales/OrderDate/Year=*/Month=*/Day=*/', 'ext_FactInternetSales', @debug_only = 1



