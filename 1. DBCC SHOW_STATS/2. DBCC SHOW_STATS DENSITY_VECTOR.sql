-- Predicate referencing charge_dt
	SELECT Charge_no
	FROM dbo.charge AS c
	WHERE charge_dt = '1999-07-20 10:44:42.157'

DBCC SHOW_STATISTICS(N'dbo.charge', _WA_Sys_00000005_0DAF0CB0)
WITH DENSITY_VECTOR;

--Density = 1/# of distinct values in a column

--IF we create multi-column stats or have a multi-column index, what do we see?
CREATE STATISTICS charge_multi_cols ON
dbo.charge (charge_amt, statement_no, charge_dt);
GO

DBCC SHOW_STATISTICS(N'dbo.charge', charge_multi_cols)
WITH DENSITY_VECTOR