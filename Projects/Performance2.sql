DBCC DROPCLEANBUFFERS
DBCC FREEPROCCACHE
------------------------------------------
--IO and CPU time information
SET STATISTICS IO ON
SET STATISTICS TIME ON
------------------------------------------
select ID from EmployeeShift

--Keep execution plan on