--Set all databases to SIMPLE RECOVERY--
set quoted_identifier on
EXEC sp_msforeachdb "
IF '?' not in ('tempdb')
begin
    exec ('ALTER DATABASE [?] SET RECOVERY SIMPLE;')
end
"
GO 

--Shrink Log file from each DB--
Declare @command varchar(MAX) 
set @command = 'Use [?]; 
declare @fileName varchar(20) 
select @fileName = FILE_NAME(2) 
DBCC Shrinkfile(@fileName,100)' 
EXECUTE sp_MSforeachdb @command




