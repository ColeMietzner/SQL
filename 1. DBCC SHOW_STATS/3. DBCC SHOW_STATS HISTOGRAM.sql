USE SQL_Tuning

-- Predicate referencing charge_dt
	SELECT Charge_no
	FROM dbo.charge AS c
	WHERE charge_dt = '1999-07-20 10:44:42.157'

--USE DBCC SHOW_STATS with HISTOGRAM
DBCC SHOW_STATISTICS(N'dbo.charge',_WA_Sys_00000005_0DAF0CB0)
WITH HISTOGRAM;
GO

/* 
	RANGE_HI_KEY = Upper-bound column value for a step
	Range_Rows = # of rows with a value falling within a histogram step, excluding the upper bound
	EQ_Rows = # of rows whose value equals the RANGE_HI_KEY
	Distinct_Range_Rows = # rows with a distinct column value within a histogram step, excluding the upper bound
	Range_HI_KEY is * always * based on leftmost column
*/

CREATE STATISTICS charge_multi_cols ON
dbo.charge (charge_amt, statement_no, charge_dt);
GO

--USE DBCC SHOW_STATS with HISTOGRAM
DBCC SHOW_STATISTICS(N'dbo.charge',charge_multi_cols)
WITH HISTOGRAM;
GO