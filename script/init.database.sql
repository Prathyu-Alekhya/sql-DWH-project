/*
=======================================
Create Database and Schemas
=======================================
Script Purpose:
	This script creates a new database named 'DataWareHouse' after checking if it already exists.
	If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database:'Bronze','silver', and 'Gold'
Warning:
	Running this script will drop the entire 'DataWareHouse' database if it exists.
	All data in the database will be permanently deleted. Proceed with the caution and ensure you have proper backups b4 running this script.
*/

use master;
Go

--Drop and recreate the 'DataWareHouse' database

IF EXISTS (Select 1 from sys.databases where name='DataWareHouse')
Begin
	Alter database DataWareHouse SET single_user with Rollback immediate;
	Drop database DataWareHouse;
end;
Go
--Create database "DataWareHouse"

Create database DataWareHouse;

--Create schema Bronze

use DataWareHouse;

create schema Bronze;
Go
create schema Siver;
Go
create schema Gold;
Go
