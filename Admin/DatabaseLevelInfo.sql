/*
	The following T-SQL statement gives information on the database names, their compatibility level and also the recovery model 
	and their current status. The result from this T-SQL Statement will help you to determine if there is any compatibility 
	level update necessary. When upgrading from an older version to new version, the compatibility level of the database may not be in the desired level. 
	The following statement will help you to list all of the database names with compatibilty level. It also lists the online/offline status of the database 
	as well as helping the DBA to see if any update to recovery model is necessary. */


		SELECT  name ,
				compatibility_level ,
				recovery_model_desc ,
				state_desc
		FROM    sys.databases
		ORDER BY name