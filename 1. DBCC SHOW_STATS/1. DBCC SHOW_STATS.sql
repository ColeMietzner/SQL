DBCC SHOW_STATISTICS

USE SQL_Tuning
GO
--Check Statistics on the dbo.charge Table
SELECT s.object_id, s.name, s.auto_created, COL_NAME(s.object_id, sc.column_id) AS col_name
FROM sys.stats AS s
	INNER JOIN sys.stats_columns AS sc
	ON s.stats_id = sc.stats_id AND s.OBJECT_ID = sc.object_id
	WHERE s.object_id = OBJECT_ID('dbo.charge');

-- Predicate referencing charge_dt
	SELECT Charge_no
	FROM dbo.charge AS c
	WHERE charge_dt = '1999-07-20 10:44:42.157'

--Check Statistics again
SELECT s.object_id, s.name, s.auto_created, COL_NAME(s.object_id, sc.column_id) AS col_name
FROM sys.stats AS s
	INNER JOIN sys.stats_columns AS sc
	ON s.stats_id = sc.stats_id AND s.OBJECT_ID = sc.object_id
	WHERE s.object_id = OBJECT_ID('dbo.charge');

--DBCC SHOW_STATISTICS(TableName, StatObjectName)
DBCC SHOW_STATISTICS(N'dbo.charge', _WA_Sys_00000005_0DAF0CB0)
WITH STAT_HEADER
/* Note Last updated, rows, rows sampled, sampled.
Updated = LAST TIME STATS were updated
Rows = Row count when Stats were updated
Rows_Sampled = total number of rows used for stats
Steps = total histogram steps sampled
						