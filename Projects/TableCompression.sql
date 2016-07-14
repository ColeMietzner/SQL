DECLARE @TableName VARCHAR(50)
DECLARE @id VARCHAR(50) 
DECLARE @numRows VARCHAR(50) 
DECLARE db_cursor CURSOR FOR 

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

OPEN db_cursor 
FETCH NEXT FROM db_cursor INTO @TableName, @id, @numRows 

WHILE @@FETCH_STATUS = 0    
BEGIN
EXEC
( 
	'ALTER TABLE [dbo].[' + @TableName + '] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)'
)
PRINT
( 
	'The Table ' + @TableName + ' has been successfully compressed with (DATA_COMPRESSION = PAGE)'
)
FETCH NEXT FROM db_cursor INTO @TableName, @id, @numRows 
END

CLOSE db_cursor 
DEALLOCATE db_cursor