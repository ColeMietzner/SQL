/* The next level of information that is important for a SQL Server database administrator to know is the location of all the backup files.
	 You don’t want the backups to go to the local drive or to an OS drive. The following T-SQL statement gets all the information related 
	 to the current backup location from the msdb database. */

SELECT DISTINCT
        physical_device_name
FROM    msdb.dbo.backupmediafamily

