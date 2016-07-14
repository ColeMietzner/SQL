USE SQL_Tuning;
GO

--include actual execution plan
SELECT charge_no
FROM dbo.charge
WHERE charge_amt = 23.99;

SELECT s.object_id, s.name, s.auto_created, COL_NAME(s.object_id, sc.column_id) AS col_name
FROM sys.stats AS s
	INNER JOIN sys.stats_columns AS sc
	ON s.stats_id = sc.stats_id AND s.OBJECT_ID = sc.object_id
	WHERE s.object_id = OBJECT_ID('dbo.charge');

-- The 303.781 estimated rows for the clustered index scan
DBCC SHOW_STATISTICS(charge, _WA_Sys_00000006_0DAF0CB0)

-- Average_Range_Rows = 303.7806 
-- Notice the 22 DISTINCT_RANGE_ROWS. Was the sampling accurate?
SELECT DISTINCT charge_amt
FROM dbo.charge
WHERE charge_amt > 1.0 AND charge_amt < 24.0;

-- but * anything in that range * gets that AVERAGE_RANGE_ROWS estimate
