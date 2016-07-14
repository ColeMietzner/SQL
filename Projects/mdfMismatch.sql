sp_configure 'xp_cmdshell', 1
reconfigure with override

create table #temp (lineText varchar(MAX) null)
insert into #temp exec xp_cmdshell 'dir G:\*'
insert into #temp exec xp_cmdshell 'dir H:\*'
insert into #temp exec xp_cmdshell 'dir J:\*'


select * from #temp where linetext not in (select linetext FROM #temp join master..sysdatabases on lineText like '% ' + substring(filename, 15,1024) + '%')
DROP TABLE #temp