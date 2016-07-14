/*Creation of a database to 9.x*/

Use Master
GO
	IF EXISTS (SELECT * from sys.databases where name = 'Support_Starwood_9_01')
		BEGIN
			ALTER DATABASE [Support_Starwood_9_01]
			SET SINGLE_USER
			WITH ROLLBACK IMMEDIATE;
			DROP DATABASE [Support_Starwood_9_01];
		END
RESTORE DATABASE [Support_Starwood_9_01] FROM  DISK = N'L:\starwood80_backup_2014_09_26_230001_6818496.bak' WITH  FILE = 1,  
MOVE N'starwood80' TO N'G:\Support_Starwood_9_01.mdf',  
MOVE N'starwood80_log' TO N'i:\Support_Starwood_9_01_1.LDF',  NOUNLOAD,  STATS = 10
GO

declare @UserID int
Declare @DBID int
Declare @UserName varchar (255)
Declare @DBName varchar (255)
Declare @DBDesc varchar (255)
set @UserName = 'Support'
set @DBName = 'Support_Starwood_9_01'
set @DBDesc = 'Support Starwood 901'
if exists (select id from Uniportal..WatsonDatabase where name = @DBDesc)
                select @DBID = id from Uniportal..WatsonDatabase where name = @DBDesc
else
BEGIN
                insert into Uniportal..WatsonDatabase values (@DBDesc, '//CTx-DW-SQL-08-1/' + @DBName, 'sa', 'CCADBA97C4AA632310147CD62F227F35', 'mssql', NULL, 0, Null, Null)
                select @DBID = @@Identity
                
END
if exists (select id from Uniportal..PortalUser where LoginID = @UserName)
                select @UserID = id from Uniportal..PortalUser where LoginID = @UserName
else
BEGIN
                insert into Uniportal..PortalUser values (@UserName, 'DB11EB12E075A47BE72A23FF5CD63D07', 'cmietzner@unifocus.com', 1, 'Y', 1, 'Red', Null, Null, 'en_US', 1, @DBID, 'N', Null)
                select @UserID = @@Identity
END
if not exists (select UserID from Uniportal..UserDatabase where UserID = @UserID and DatabaseID = @DBID)
                insert into Uniportal..UserDatabase values (@UserID, @DBID)
exec('if not exists (select id from ' + @DBName + '..WatsonUser where Name = ''' + @UserName + ''') 
                insert into ' + @DBName + '..WatsonUser (Name, Flags) values (''' + @UserName + ''', 1)')