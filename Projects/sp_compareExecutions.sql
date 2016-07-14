USE [PerformanceLog]
GO

/****** Object:  StoredProcedure [dbo].[usp_compareexecutions]    Script Date: 12/1/2014 1:48:42 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROC [dbo].[usp_compareexecutions] (@baselineExecutionId INT, @comparisonExecutionId INT) AS
BEGIN
	WITH temp AS
	(
		SELECT 
			S.executionId, B.description, B.command, B.commandParameters, S.dateTimeStarted,
			B.averageDuration AS baselineDuration, 
			(SELECT acceptableIncreasePercent + 1 FROM acceptableDurationCriteria WHERE B.averageDuration BETWEEN durationFrom AND durationTo) * B.averageDuration AS acceptableDurationIncrease,
			B.averageLatency AS baselineLatency, 
			(SELECT acceptableIncreasePercent + 1 FROM acceptableLatencyCriteria WHERE B.averageLatency BETWEEN latencyFrom AND latencyTo) * B.averageLatency AS acceptableLatencyIncrease,
			B.sampleCount AS baselineSampleCount, 
			S.averageDuration, S.averageLatency, S.sampleCount,
			S.averageDuration / (B.averageDuration * 1.0) - 1.0 AS percentDurationIncrease,
			S.averageLatency / (B.averageLatency * 1.0) - 1.0 AS percentLatencyIncrease

		FROM 
			statisticsView B JOIN statisticsView S ON B.executionId = @baselineExecutionId AND S.executionId = @comparisonExecutionId AND B.command = S.command AND B.commandParameters = S.commandParameters
	), comparison AS 
	(
	SELECT 
		executionId, description, command,commandParameters, dateTimeStarted, baselineDuration, 
		CASE WHEN acceptableDurationIncrease < 250 THEN 250 ELSE acceptableDurationIncrease END AS acceptableDurationThreshold, baselineLatency,
		CASE WHEN acceptableLatencyIncrease < 250 THEN 250 ELSE acceptableLatencyIncrease END AS acceptableLatencyThreshold, 
		baselineSampleCount, averageDuration, averageLatency, sampleCount, percentDurationIncrease, percentLatencyIncrease
		FROM temp
	)
	SELECT 
		executionId, description, command, commandParameters, datetimestarted, baselineDuration, acceptableDurationThreshold, baselineLatency, acceptableLatencyThreshold, 
		baselineSampleCount, averageDuration, averageLatency, sampleCount,
	percentDurationIncrease, CASE WHEN acceptableDurationThreshold < averageDuration THEN 'Y' ELSE 'N' END AS durationOverLimits,
	percentLatencyIncrease, CASE WHEN acceptableLatencyThreshold < averageLatency THEN 'Y' ELSE 'N' END AS latencyOverLimits
	FROM comparison
	
END
GO


