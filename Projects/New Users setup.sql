declare @UserID int
Declare @DBID int
Declare @UserName varchar (255)
Declare @DBName varchar (255)
Declare @DBDesc varchar (255)
DECLARE @UserPassword VARCHAR(255)

--Username(if this does not exist it will create one for you)
set @UserName = 'cm9x'
--Exact dbname
set @DBName = 'Cole_FairmontDel2_9x'
--Unique DB description. This cannot be anything that exist already or your login will get corrupt. No special characters wither.
set @DBDesc = 'Cole Fairmont Del 2 9x'
SET @UserPassword = 'E5C703BF7D9C3D1B'

if exists (select id from Uniportal..WatsonDatabase where name = @DBDesc)
select @DBID = id from Uniportal..WatsonDatabase where name = @DBDesc
else
BEGIN

                insert into Uniportal..WatsonDatabase values (@DBDesc, '//ctx-du-sql-002/' + @DBName, 'qaAdmin', '13AD4B93F5549DE4E72A23FF5CD63D07', 'mssql', NULL, NULL, NULL, NULL)
                select @DBID = @@Identity


END

if exists (select id from Uniportal..PortalUser where LoginID = @UserName)

                select @UserID = id from Uniportal..PortalUser where LoginID = @UserName

else

BEGIN

--Here you will need to create a password for each database user. You will have to run the EncryptFrame Class.
--This class is a main class and can be run through intelliJ. The color is the answer to your reset password question, "What is your favorite Color?"
                insert into Uniportal..PortalUser values (@UserName, @UserPassword, 'cmietzner@unifocus.com', 1, 'Y', 1, 'Red', Null, Null, 'en_US', 1, @DBID, 'N', NULL)

                select @UserID = @@Identity

END

if not exists (select UserID from Uniportal..UserDatabase where UserID = @UserID and DatabaseID = @DBID)

                insert into Uniportal..UserDatabase values (@UserID, @DBID)

exec('if not exists (select id from ' + @DBName + '..WatsonUser where Name = ''' + @UserName + ''')

                insert into ' + @DBName + '..WatsonUser (Name, Flags) values (''' + @UserName + ''', 1)')

