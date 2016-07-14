/* The following T-SQL Statement lists all of the databases in the server and the last day the backup happened. 
	This will help the database administrators to check the backup jobs and also to make sure backups are happening for all the databases. */

SELECT  db.name ,
        CASE WHEN MAX(b.backup_finish_date) IS NULL THEN 'No Backup'
             ELSE CONVERT(VARCHAR(100), MAX(b.backup_finish_date))
        END AS last_backup_finish_date
FROM    sys.databases db
        LEFT OUTER JOIN msdb.dbo.backupset b ON db.name = b.database_name
                                                AND b.type = 'D'
WHERE   db.database_id NOT IN ( 2 )
GROUP BY db.name
ORDER BY 2 DESC