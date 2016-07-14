SELECT * FROM dbo.executions

usp_compareExecutions 68,69


SELECT  command ,
        baselineduration, Averageduration,
        baselineLatency, Averagelatency 
FROM    OPENQUERY([DTX-DW-SQL-001],
                  'PerformanceLog..usp_compareexecutions 68,69') 
WHERE	durationOverLimits = 'Y' OR LatencyOverLimits = 'Y'