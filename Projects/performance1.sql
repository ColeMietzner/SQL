Select * from  sys.dm_exec_requests
Select * from sys.dm_exec_Procedure_stats as dep

sp_whoisactive

DBCC INPUTBUFFER()

EXEC sp_whoisactive
@get_plans = 1, @get_transaction_info = 1
