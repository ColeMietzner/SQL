 /* 
 The following T-SQL Statement provides the logical name and the physical 
 location of the data/log files of all the databases available in the current SQL Server instance.
  */

SELECT  DB_NAME(database_id) AS DatabaseName ,
        name ,
        type_desc ,
        physical_name
FROM    sys.master_files
