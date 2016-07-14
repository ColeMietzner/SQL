USE uniportal
WITH dbAccess AS
	(
		SELECT databaseURL, MAX(LoginDateTime) AS LastAccess 
		FROM WatsonDatabase 
		JOIN LoginHistory ON watsonDatabase.ID = LoginHistory.DatabaseID 
		GROUP BY DatabaseURL
	)
SELECT * FROM dbAccess 
WHERE LastAccess < GETDATE() - 60 
order by dbAccess.LastAccess,databaseURL

