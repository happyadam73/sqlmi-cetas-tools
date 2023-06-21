/*

PRE-REQUISITES
==============

1. You will need to create or use an existing Azure Blob Storage Account Container.  Configure the parameters below with the name of the storage account, and the container
2. Assign the Storage Blob Data Contributor Role of your Blob Storage account to the Azure SQL Managed Instance Managed Identity
3. Enable Polybase Export for your Azure SQL Managed Instance - see below
4. Populate the parameters at the top of this script, and change the database context to the Database containing the tables to export

Enable Polybase Export on your Azure SQL Managed Instance
---------------------------------------------------------
Run the following command using the Azure CLI, or you can do this with CloudShell within the Azure Portal

az sql mi server-configuration-option set --resource-group '<sqlmi resource group>' --managed-instance-name '<sqlmi name>' --name 'allowPolybaseExport' --value '1'

Further documentation can be found here:
https://learn.microsoft.com/en-us/sql/t-sql/statements/create-external-table-as-select-transact-sql?view=azuresqldb-mi-current

*/

-- Populate the following parameters
DECLARE @storage_account	VARCHAR(100) = 'saawbblobdevuks2',
        @storage_container	VARCHAR(100) = 'raw';

-- IMPORTANT: Don't change any of the code below

-- Check we're currently in the context of a user database.  If you get this error, simply change the database context to the database which contains the data you want to export
IF DB_NAME() IN ('master', 'model', 'msdb', 'tempdb')
    THROW 50000, 'You need to run this script in the context of a user database', 1;

DECLARE @db_name                SYSNAME         = DB_NAME(),
		@sql_cmd                NVARCHAR(MAX),
		@master_key_password    NVARCHAR(50),
		@credential_name        VARCHAR(200)    = @storage_account + '-' + @storage_container,
		@data_source_name       VARCHAR(350)    = @storage_account + '-' + @storage_container + '-' + DB_NAME();

-- Generate a database master key if it doesn't exist; use a random strong password
IF NOT EXISTS(SELECT 1 FROM sys.symmetric_keys WHERE name LIKE '%DatabaseMasterKey%')
BEGIN
    SELECT @master_key_password = CAST(N'' AS XML).value('xs:base64Binary(xs:hexBinary(sql:column("bin")))', 'VARCHAR(MAX)') FROM (SELECT CONVERT(VARBINARY(MAX), CAST(CRYPT_GEN_RANDOM(17) AS NVARCHAR(50))) AS bin) AS x;
    SET @sql_cmd = N'CREATE MASTER KEY ENCRYPTION BY PASSWORD = ''' + @master_key_password + '''';
    EXEC sp_executesql @sql_cmd;
END

-- Generate a Managed Identity credential for the storage account
SET @sql_cmd = N'
IF NOT EXISTS(SELECT 1 FROM sys.database_scoped_credentials WHERE [name] = ''' + @credential_name + ''')
    CREATE DATABASE SCOPED CREDENTIAL [' + @credential_name + ']
    WITH IDENTITY = ''Managed Identity''; 
';
EXEC sp_executesql @sql_cmd;

-- Create an external data source pointing to the file path, and referencing database-scoped credential
-- By convention we use the database name as root folder within the file path
SET @sql_cmd = N'
IF NOT EXISTS(SELECT 1 FROM sys.external_data_sources WHERE [name] = ''' + @data_source_name + ''')
    CREATE EXTERNAL DATA SOURCE [' + @data_source_name + ']
    WITH (
        LOCATION = ''abs://' + @storage_container + '@' + @storage_account + '.blob.core.windows.net/' + @db_name + ''',
        CREDENTIAL = [' + @credential_name + ']
    );
';
EXEC sp_executesql @sql_cmd;

-- Create External File Format for PARQUET with Snappy compression
IF NOT EXISTS(SELECT 1 FROM sys.external_file_formats WHERE [name] = 'ParquetFileFormat')
    CREATE EXTERNAL FILE FORMAT ParquetFileFormat
    WITH (
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    );

-- Create cetas schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'cetas')
    EXEC sp_executesql N'CREATE SCHEMA [cetas]';

-- We need to reference the data source in various procs, so create a 'global' default that other procs/functions can use if not provided
-- Do this by creating a UDF based on the @data_source_name parameter above
SET @sql_cmd = N'
CREATE OR ALTER FUNCTION cetas.ExternalDataSource()
RETURNS SYSNAME AS
BEGIN
    RETURN N''' + @data_source_name + ''';
END
';
EXEC sp_executesql @sql_cmd;

-- Drop/Create Proc to generate a CREATE EXTERNAL TABLE script based on the object name including partition columns based on specified date type column
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND [object_id] = OBJECT_ID('cetas.usp_CreateExternalTableFromSourceTable'))
    DROP PROCEDURE [cetas].[usp_CreateExternalTableFromSourceTable];
GO

-- Create Stored procedure to generate the parent external table to virtualise specified source data table data
CREATE PROCEDURE [cetas].[usp_CreateExternalTableFromSourceTable]
    @object_name                NVARCHAR(512),
    @partition_date_column      SYSNAME,
    @create_external_table_sql  NVARCHAR(MAX)   = NULL OUTPUT,
    @data_source_name           VARCHAR(350)    = NULL,
    @drop_existing              BIT             = 1,
    @debug_only                 BIT             = 0
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @schema_name            SYSNAME,
			@table_name             SYSNAME,
			@columns_sql            NVARCHAR(MAX),
			@external_table_name    VARCHAR(256);

    -- extract schema and table/view name from the object name
    SELECT @schema_name = PARSENAME(@object_name, 2), @table_name = PARSENAME(@object_name, 1);

    -- If no external data source specified then set this to the global default
    SELECT @data_source_name = ISNULL(@data_source_name, cetas.ExternalDataSource());

    -- Set External Table name - we use the partition column in the name so it doesn't clash with creating from the same table but a different partition column
    SET @external_table_name = @table_name + 'ExternalPartitionedBy' + @partition_date_column;

    SELECT @columns_sql = 
        STRING_AGG(
            CONVERT(NVARCHAR(MAX),
            CHAR(9) + '[' + COLUMN_NAME + '] ' + 
            UPPER(DATA_TYPE) + ISNULL('(' + CASE WHEN CHARACTER_MAXIMUM_LENGTH < 0 THEN 'MAX' ELSE CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR(10)) END + ')','') + ' ' + 
            CASE WHEN IS_NULLABLE = 'YES' THEN 'NULL' ELSE 'NOT NULL' END),
            ',' + CHAR(13) + CHAR(10)
        ) WITHIN GROUP (ORDER BY ORDINAL_POSITION) + ',' + CHAR(13) + CHAR(10) + 
        CHAR(9) + '[' + @partition_date_column + 'Year] INT NOT NULL,' + CHAR(13) + CHAR(10) + 
        CHAR(9) + '[' + @partition_date_column + 'Month] INT NOT NULL,' + CHAR(13) + CHAR(10) + 
        CHAR(9) + '[' + @partition_date_column + 'Day] INT NOT NULL'
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
        TABLE_SCHEMA = @schema_name
    AND TABLE_NAME = @table_name;

    SET @create_external_table_sql = N'';

	IF @drop_existing = 1
		SET @create_external_table_sql = @create_external_table_sql + N'
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [object_id] = OBJECT_ID(''' + @schema_name + '.' + @external_table_name + '''))
	DROP EXTERNAL TABLE [' + @schema_name + '].[' + @external_table_name + '];
';

	SET @create_external_table_sql = @create_external_table_sql + N'
CREATE EXTERNAL TABLE [' + @schema_name + '].[' + @external_table_name + '] (
' + @columns_sql + '
)
WITH (
		LOCATION = ''' + @schema_name + '/' + @table_name + '/' + @partition_date_column + '/Year=*/Month=*/Day=*/*.parquet'',
		DATA_SOURCE = [' + @data_source_name + '],
		FILE_FORMAT = ParquetFileFormat,
		PARTITION (
			[' + @partition_date_column + 'Year],
			[' + @partition_date_column + 'Month],
			[' + @partition_date_column + 'Day]
		) 
);
';

	IF @debug_only = 0
		EXEC sp_executesql @create_external_table_sql;

END;
GO


-- Drop/Create Proc to populate the external table using CETAS to generate the underlying Parquet data
-- Data is loaded based on incremental loading via partition date column keys (i.e. year/month/day)
-- This is expected to be run on a daily basis
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND [object_id] = OBJECT_ID('cetas.usp_LoadExternalTableFromSourceTableData'))
    DROP PROCEDURE [cetas].[usp_LoadExternalTableFromSourceTableData];
GO

-- Create Stored procedure to populate the external table using CETAS to generate the underlying Parquet data
CREATE PROCEDURE [cetas].[usp_LoadExternalTableFromSourceTableData]
	@object_name				NVARCHAR(512),
	@partition_date_column		SYSNAME,
	@year						INT,
	@month						INT,
	@day						INT,
	@load_external_table_sql	NVARCHAR(MAX)   = NULL OUTPUT,
	@data_source_name			VARCHAR(350)	= NULL,
	@debug_only					BIT				= 0
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @schema_name					SYSNAME,
			@table_name						SYSNAME,
			@columns_sql					NVARCHAR(MAX),
			@external_staging_table_name	VARCHAR(256);

	-- extract schema and table/view name from the object name
	SELECT @schema_name = PARSENAME(@object_name, 2), @table_name = PARSENAME(@object_name, 1);

	-- If no external data source specified then set this to the global default
	SELECT @data_source_name = ISNULL(@data_source_name, cetas.ExternalDataSource());

	-- Set External Staging Table name - we use the partition column in the name so it doesn't clash with creating from the same table but a different partition column
	SET @external_staging_table_name = @table_name + 'ExternalPartitionedBy' + @partition_date_column + '_Staging';

	SELECT @columns_sql = 
		STRING_AGG(
			CONVERT(NVARCHAR(MAX),
			CHAR(9) + CHAR(9) + '[' + COLUMN_NAME + ']'),
			',' + CHAR(13) + CHAR(10)
		) WITHIN GROUP (ORDER BY ORDINAL_POSITION) + ',' + CHAR(13) + CHAR(10) + 
		CHAR(9) + CHAR(9) + 'YEAR([' + @partition_date_column + ']) AS [' + @partition_date_column + 'Year],' + CHAR(13) + CHAR(10) + 
		CHAR(9) + CHAR(9) + 'MONTH([' + @partition_date_column + ']) AS [' + @partition_date_column + 'Month],' + CHAR(13) + CHAR(10) + 
		CHAR(9) + CHAR(9) + 'DAY([' + @partition_date_column + ']) AS [' + @partition_date_column + 'Day]'
	FROM INFORMATION_SCHEMA.COLUMNS
	WHERE
		TABLE_SCHEMA = @schema_name
	AND TABLE_NAME = @table_name;

	SET @load_external_table_sql = N'
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [object_id] = OBJECT_ID(''' + @schema_name + '.' + @external_staging_table_name + '''))
	DROP EXTERNAL TABLE [' + @schema_name + '].[' + @external_staging_table_name + '];	
	';

	SET @load_external_table_sql = @load_external_table_sql + N'
CREATE EXTERNAL TABLE [' + @schema_name + '].[' + @external_staging_table_name + '] 
WITH (
		LOCATION = ''' + @schema_name + '/' + @table_name + '/' + @partition_date_column + '/Year=' + CAST(@year AS VARCHAR(10)) + '/Month=' + CAST(@month AS VARCHAR(10)) + '/Day=' + CAST(@day AS VARCHAR(10)) + '/'',
		DATA_SOURCE = [' + @data_source_name + '],
		FILE_FORMAT = ParquetFileFormat
	) 
AS
	SELECT 
' + @columns_sql + '
	FROM [' + @schema_name + '].[' + @table_name + ']
	WHERE
		YEAR([' + @partition_date_column + ']) = ' + CAST(@year AS VARCHAR(10)) + ' 
	AND MONTH([' + @partition_date_column + ']) = ' + CAST(@month AS VARCHAR(10)) + '
	AND DAY([' + @partition_date_column + ']) = ' + CAST(@day AS VARCHAR(10)) + ';
';

	IF @debug_only = 0
		EXEC sp_executesql @load_external_table_sql;

END;
GO

/*
EXEC [cetas].[usp_CreateExternalTableFromSourceTable] '[dbo].[FactInternetSales]', 'OrderDate';
EXEC [cetas].[usp_LoadExternalTableFromSourceTableData] '[dbo].[FactInternetSales]', 'OrderDate', 2013, 12, 1;
EXEC [cetas].[usp_LoadExternalTableFromSourceTableData] '[dbo].[FactInternetSales]', 'OrderDate', 2013, 12, 2;
EXEC [cetas].[usp_LoadExternalTableFromSourceTableData] '[dbo].[FactInternetSales]', 'OrderDate', 2013, 12, 3;
EXEC [cetas].[usp_LoadExternalTableFromSourceTableData] '[dbo].[FactInternetSales]', 'OrderDate', 2013, 12, 4;
EXEC [cetas].[usp_LoadExternalTableFromSourceTableData] '[dbo].[FactInternetSales]', 'OrderDate', 2013, 12, 5;

EXEC [cetas].[usp_CreateExternalTableFromSourceTable] '[dbo].[FactInternetSales]', 'ShipDate';
EXEC [cetas].[usp_LoadExternalTableFromSourceTableData] '[dbo].[FactInternetSales]', 'ShipDate', 2013, 12, 1;
EXEC [cetas].[usp_LoadExternalTableFromSourceTableData] '[dbo].[FactInternetSales]', 'ShipDate', 2013, 12, 2;
EXEC [cetas].[usp_LoadExternalTableFromSourceTableData] '[dbo].[FactInternetSales]', 'ShipDate', 2013, 12, 3;
EXEC [cetas].[usp_LoadExternalTableFromSourceTableData] '[dbo].[FactInternetSales]', 'ShipDate', 2013, 12, 4;
EXEC [cetas].[usp_LoadExternalTableFromSourceTableData] '[dbo].[FactInternetSales]', 'ShipDate', 2013, 12, 5;
*/
