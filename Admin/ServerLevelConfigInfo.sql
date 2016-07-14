/* Server level configuration controls some of the features and performance of SQL Server. It is also important for a SQL Server DBA 
   to know the server level configuration information. The following SQL Statement will give all of the information related to Server level configuration*/

SELECT  *
FROM    sys.configurations
ORDER BY name