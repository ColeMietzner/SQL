use uniportal
SELECT * from PortalUser where ID not in (select distinct PortalUserID from LoginHistory where LoginDateTime < '2013/01/01') 

--Delete from PortalUser where ID not in (select distinct PortalUserID from LoginHistory where LoginDateTime > '2012/09/01') 

SELECT * FROM PortalUser where DefaultDatabaseID IN 
( 
	SELECT id from WatsonDatabase where DatabaseURL IN 
		(
			select DatabaseURL from watsonDatabase where databaseUrl LIKE '%dco%' AND DatabaseURL NOT IN 
				(
					select '//dco-d-sql-001/' + Name from sys.databases
				)
		)
)