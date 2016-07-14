SELECT TOP 100
qt.text as QUERY,
qs.execution_count,
qs.total_logical_reads, qs.last_logical_reads,
qs.min_logical_reads, qs.max_logical_reads,
qs.total_elapsed_time, qs.last_elapsed_time,
qs.min_elapsed_time, qs.max_elapsed_time,
qs.last_execution_time,
qp.query_plan
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
WHERE qt.encrypted=0
and qt.text not like '/*Debug query */%'
ORDER BY qs.last_execution_time DESC, qs.total_logical_reads DESC