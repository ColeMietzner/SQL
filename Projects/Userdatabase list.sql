use uniportal
select 
	a.loginid , b.Name, c.DatabaseID, c.userid 
from
	PortalUser a, WatsonDatabase b, UserDatabase c 
where
	a.ID = c.UserID and b.ID = c.DatabaseID 
order by
	a.loginId

DELETE from portaluser WHERE ID = 644


SELECT * from sys.databases


select 
	RIGHT(DatabaseURL, Datalength(DatabaseURL) - CHARINDEX('1/', DatabaseURL) - 1) 
from 
	watsonDatabase 
where 
	databaseUrl LIKE '%dco%' AND DatabaseURL NOT IN (select '//dco-d-sql-001/' + Name from sys.databases)

Delete from WatsonDatabase where DatabaseURL IN (select 
	DatabaseURL 
from 
	watsonDatabase 
where 
	databaseUrl LIKE '%dco%' AND DatabaseURL NOT IN (select '//dco-d-sql-001/' + Name from sys.databases))
	
	
SELECT * FROM PortalUser where DefaultDatabaseID IN ( SELECT id from WatsonDatabase where DatabaseURL IN (select 
	DatabaseURL 
from 
	watsonDatabase 
where 
	databaseUrl LIKE '%dco%' AND DatabaseURL NOT IN (select '//dco-d-sql-001/' + Name from sys.databases)))
	
