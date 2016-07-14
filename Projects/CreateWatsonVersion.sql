IF NOT EXISTS 
	(SELECT 1 from sysobjects where name = 'WatsonVersion' and xtype = 'U')
		BEGIN
			create table dbo.WatsonVersion (DatabaseVersion varchar(10) not null)
			INSERT INTO 
				dbo.WatsonVersion (DatabaseVersion) 
					select 
						max(id) as MajorVersion 
					from 
						databasechangelog
					where 
						author='MajorVersion'
		END
		

		
		