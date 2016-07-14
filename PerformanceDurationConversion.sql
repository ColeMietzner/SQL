usp_compareexecutions 66,67

SELECT  command ,
        CONVERT(TIME, DATEADD(ms, ( baselineduration - Averageduration ), 0)) AS durationIncrease ,
        baselineLatency - Averagelatency AS LatencyIncrease
FROM    OPENQUERY([DTX-DW-SQL-001],
                  'PerformanceLog..usp_compareexecutions 68,69')
WHERE   baselineduration - Averageduration > 0
ORDER BY durationIncrease DESC 

SELECT command, baselineduration - Averageduration AS durationIncrease, baselineLatency - Averagelatency AS LatencyIncrease FROM OPENQUERY([DTX-DW-SQL-001],'PerformanceLog..usp_compareexecutions 66,67') 

