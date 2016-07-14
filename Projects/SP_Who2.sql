CREATE TABLE #sp_who2 
( 
   SPID INT,  
   Status VARCHAR(1000) NULL,  
   Login SYSNAME NULL,  
   HostName SYSNAME NULL,  
   BlkBy SYSNAME NULL,  
   DBName SYSNAME NULL,  
   Command VARCHAR(1000) NULL,  
   CPUTime INT NULL,  
   DiskIO INT NULL,  
   LastBatch VARCHAR(1000) NULL,  
   ProgramName VARCHAR(1000) NULL,  
   SPID2 INT, 
   REQUESTEDID INT 
) 
GO 
INSERT INTO #sp_who2 
EXEC sp_who2 
GO 
SELECT * 
FROM #sp_who2 
WHERE DBName = 'Cole_HiltonAPAC_9x' 
GO 
DROP TABLE #sp_who2 
GO 


KILL 58
KILL 190
KILL 218