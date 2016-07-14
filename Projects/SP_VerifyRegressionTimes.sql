usp_VerifyRegressionTimes 8, '11/01/2014', '12/30/2014'

SELECT TOP 100 * FROM watsonTask ORDER BY StartDateTime DESC


SELECT AVG(DATEDIFF(SECOND, startDatetime, enddatetime)) FROM watsonTask WHERE taskname LIKE 'generateStandardHours' AND tasknotes LIKE '%Division ID:0, Department ID:0, Job ID:0, Standard Set ID:1' AND PropertyID = 1 AND StartDateTime IS NOT NULL
                                AND EndDateTime IS NOT NULL

USE Regress_HiltonUS_822

SELECT DISTINCT
        taskname ,
        TaskNotes ,
        ( TotalTime - pastAverage ) / CAST(( pastAverage ) AS FLOAT) * 100.00 AS percentIncrease
FROM    OPENQUERY([CTX-DW-SQL-08-1],
                  'Regress_hiltonus_822..usp_VerifyRegressionTimes 1, ''11/01/2014'', ''11/30/2014''')
WHERE   ( TotalTime - pastAverage ) / CAST(( pastAverage ) AS FLOAT) * 100.00 > 100 AND completedby = 9194
ORDER BY percentIncrease DESC

EXEC sp_serveroption 'ctx-dw-sql-08-1', 'DATA ACCESS', TRUE



SELECT DISTINCT
        taskname ,
        TaskNotes ,
        ( TotalTime - pastAverage ) / CAST(( pastAverage ) AS FLOAT) * 100.00 AS percentIncrease
FROM    OPENQUERY([CTX-DW-SQL-08-1],
                  'Regress_hiltonus_822..usp_VerifyRegressionTimes 1, ''11/01/2014'', ''11/30/2014''')
WHERE  completedby = 9194
ORDER BY percentIncrease DESC

SELECT *
FROM    OPENQUERY([CTX-DW-SQL-08-1],
                  'Regress_hiltonus_822..usp_VerifyRegressionTimes 1, ''11/01/2014'', ''11/30/2014''')
WHERE  completedby = 9194
