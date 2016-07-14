ALTER PROCEDURE usp_verifyRegressionTimes

@propertyid INT,
@userID INT

AS (

SELECT  taskName , TaskNotes, completedBy,
                                AVG(DATEDIFF(SECOND, StartDateTime,
                                             EndDateTime)) AS 'Total Time (s)'
                       FROM     watsonTask
                       WHERE    propertyid = @propertyID
                                AND StartDateTime IS NOT NULL
                                AND EndDateTime IS NOT NULL
								AND CompletedBy = @userId
								AND StackTrace IS NULL
								--AND tasknotes LIKE '%Division ID:0, Department ID:0, Job ID:0, Standard Set ID:1'
                       GROUP BY TaskName, TaskNotes, completedby 
                       HAVING   AVG(DATEDIFF(SECOND, StartDateTime,
                                             EndDateTime)) > 0


)
