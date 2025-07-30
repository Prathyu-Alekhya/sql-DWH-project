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
