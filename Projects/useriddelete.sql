use uniportal
SELECT * from PortalUser where ID not in (select distinct PortalUserID from LoginHistory where LoginDateTime > '2015/01/01') ORDER BY LoginId

--Delete from PortalUser where ID not in (select distinct PortalUserID from LoginHistory where LoginDateTime > '2015/01/01') 

SELECT * FROM PortalUser where DefaultDatabaseID IN 
( 
	SELECT id from WatsonDatabase where DatabaseURL IN 
		(
			select DatabaseURL from watsonDatabase where databaseUrl LIKE '%ctx%' AND DatabaseURL NOT IN 
				(
					select '//ctx-du-sql-002/' + Name from sys.databases
				)
		)
)
