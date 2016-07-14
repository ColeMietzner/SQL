DECLARE @IndexName VARCHAR(50) 
DECLARE @TableName VARCHAR(50) 
DECLARE @numRows VARCHAR(50) 
DECLARE db_cursor CURSOR FOR 

WITH TopTen as (
SELECT TOP 10
    so.name, 
    so.id,
    MAX(si.rows) as numberRows
FROM 
    sysobjects so, 
    sysindexes si 
WHERE 
    so.xtype = 'U' 
    AND 
    si.id = OBJECT_ID(so.name) 
group by
      so.id, so.Name
order by
      3 desc
)

select i.name as IndexName, max(topten.name) as TableName, COUNT(*) as NumOfOccurances
from topTen inner join sys.indexes i on i.object_id = topten.id
inner join sys.index_columns ic on ic.index_id = i.index_id and ic.object_id = topten.id
        inner join sys.columns c on c.object_id = topten.id and
                ic.column_id = c.column_id

where i.index_id > 0    
and i.type in (2) -- clustered & nonclustered only
and i.is_primary_key = 0 -- do not include PK indexes
and i.is_unique_constraint = 0 -- do not include UQ
and i.is_disabled = 0
and i.is_hypothetical = 0
and ic.key_ordinal > 0
group by i.name
having COUNT(*) > 1

OPEN db_cursor 
FETCH NEXT FROM db_cursor INTO @IndexName, @TableName, @numRows 

WHILE @@FETCH_STATUS = 0    
BEGIN
EXEC
( 
'ALTER INDEX [' + @IndexName + '] ON [dbo].[' + @TableName + '] REBUILD PARTITION = ALL WITH 
	( 
		FILLFACTOR = 80, PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON, ONLINE = OFF,SORT_IN_TEMPDB = OFF, DATA_COMPRESSION = PAGE 
	)'
)
PRINT
( 
	'The INDEX ' + @IndexName + ' has been successfully compressed with (DATA_COMPRESSION = PAGE)'
)
FETCH NEXT FROM db_cursor INTO @IndexName, @TableName, @numRows 
END

CLOSE db_cursor 
DEALLOCATE db_cursor

