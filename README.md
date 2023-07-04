# sqlmi-cetas-tools
Collection of SQL Scripts to provide additional data virtualization functionality for Azure SQL Managed Instance with use of CETAS

## Pre-requisites

1. You will need to create or use an existing Azure Blob Storage Account Container.  Configure the parameters below with the name of the storage account, and the container
2. Assign the Storage Blob Data Contributor Role of your Blob Storage account to the Azure SQL Managed Instance Managed Identity
3. Enable Polybase Export for your Azure SQL Managed Instance - see below
4. Populate the parameters at the top of the sql/DeployCETASTablePartitioning.sql script, and change the database context to the Database containing the tables to export

Enable Polybase Export on your Azure SQL Managed Instance
---------------------------------------------------------
Run the following command using the Azure CLI, or you can do this with CloudShell within the Azure Portal

az sql mi server-configuration-option set --resource-group '<sqlmi resource group>' --managed-instance-name '<sqlmi name>' --name 'allowPolybaseExport' --value '1'

Further documentation can be found here:
https://learn.microsoft.com/en-us/sql/t-sql/statements/create-external-table-as-select-transact-sql?view=azuresqldb-mi-current
