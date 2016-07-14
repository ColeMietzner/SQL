USE SQL_Tuning;
GO

--include actual execution plan
SELECT charge_no
FROM dbo.charge
WHERE charge_amt = 50.00;

set NOCOUNT ON;
GO

INSERT dbo.charge
        ( member_no ,
          provider_no ,
          category_no ,
          charge_dt ,
          charge_amt ,
          statement_no ,
          charge_code
        )
VALUES  ( 8842 , -- member_no - numeric_id
          484 , -- provider_no - numeric_id
          2 , -- category_no - numeric_id
          '2014-04-06 01:01:21', -- charge_dt - datetime
          50.00 , -- charge_amt - money
          5561 , -- statement_no - numeric_id
          '' -- charge_code - status_code
        );
GO 10000

--statistics for charge_amt?
SELECT s.object_id, s.name, s.auto_created, COL_NAME(s.object_id, sc.column_id) AS col_name
FROM sys.stats AS s
	INNER JOIN sys.stats_columns AS sc
	ON s.stats_id = sc.stats_id AND s.OBJECT_ID = sc.object_id
	WHERE s.object_id = OBJECT_ID('dbo.charge');

--the 513.474 estimated rows for the Clustered Index Scan suggests we used sampled statistics. Did we?
DBCC SHOW_STATISTICS(charge, _WA_Sys_00000006_0DAF0CB0)

SELECT 513.4741/1600000
--selectivity = 0.000320921312

SELECT 0.000320921312 * (SELECT COUNT(*) FROM dbo.charge)
--516.683312320000 <-- Estimated # rows from execution plan after add 10000 new rows.

/* So we take the selectivity of the predicate multiplied times the * current * table cardinality*/
