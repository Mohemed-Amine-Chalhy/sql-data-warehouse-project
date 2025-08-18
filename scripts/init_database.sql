/*
=====================================================
Create Database and Schemas
=====================================================
Purpose:
		This script will check for the existence of the DataWarehouse database,
		drop it if it exists, and then create a new DataWarehouse database with bronze,
		silver, and gold schemas.

Warning:
		This script will drop the entire DataWarehouse database and all of its contents.
		This action is permanent and cannot be undone.
=====================================================
*/




USE master;

IF EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    DROP DATABASE DataWarehouse;
END
GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
