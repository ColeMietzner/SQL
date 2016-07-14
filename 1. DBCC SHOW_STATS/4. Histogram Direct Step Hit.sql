USE SQL_Tuning;
GO

--include actual execution plan
SELECT charge_no
FROM dbo.charge
WHERE charge_amt = 50.00;

--statistics for charge_amt?
SELECT s.object_id, s.name, s.auto_created, COL_NAME(s.object_id, sc.column_id) AS col_name
FROM sys.stats AS s
	INNER JOIN sys.stats_columns AS sc
	ON s.stats_id = sc.stats_id AND s.OBJECT_ID = sc.object_id
	WHERE s.object_id = OBJECT_ID('dbo.charge');

--the 513.474 estimated rows for the Clustered Index Scan suggests we used sampled statistics. Did we?
DBCC SHOW_STATISTICS(charge, _WA_Sys_00000006_0DAF0CB0)