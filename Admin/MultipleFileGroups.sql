/*A database may contain filegroups other than the primary file group. The following T-SQL Statement 
	gets executed in each database on the server and displays the file groups related results. */
EXEC master.dbo.sp_MSforeachdb @command1 = 'USE [?] SELECT * FROM sys.filegroups'