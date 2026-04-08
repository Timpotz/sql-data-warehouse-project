-- Switch to the system database first.
-- This is a safe place to create a new user database from.
USE master;
GO

-- Check whether the database already exists.
-- DB_ID() returns the database ID if found, otherwise NULL.
IF DB_ID(N'DataWarehouse') IS NULL
BEGIN
    -- Create the database only if it does not already exist.
    CREATE DATABASE DataWarehouse;
END;
GO

-- Switch context into the new database.
-- Without this, the schemas may be created in the wrong database.
USE DataWarehouse;
GO

-- Check whether the bronze schema already exists.
-- SCHEMA_ID() returns the schema ID if found, otherwise NULL.
IF SCHEMA_ID(N'bronze') IS NULL
BEGIN
    -- Create the bronze schema.
    -- EXEC() is used because CREATE SCHEMA is safer in dynamic SQL
    -- when used inside an IF block in SQL Server.
    EXEC(N'CREATE SCHEMA bronze');
END;
GO

-- Check whether the silver schema already exists.
IF SCHEMA_ID(N'silver') IS NULL
BEGIN
    -- Create the silver schema only if it does not exist.
    EXEC(N'CREATE SCHEMA silver');
END;
GO

-- Check whether the gold schema already exists.
IF SCHEMA_ID(N'gold') IS NULL
BEGIN
    -- Create the gold schema only if it does not exist.
    EXEC(N'CREATE SCHEMA gold');
END;
GO