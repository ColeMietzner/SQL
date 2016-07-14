--SELECT DISTINCT(taskNotes) FROM watsonTask WHERE taskname LIKE 'reportRan' 

SELECT  COUNT(*) AS TimesRun ,
        tasknotes
FROM    watsonTask
WHERE   taskname LIKE 'reportRan'
        AND StartDateTime BETWEEN '01/01/2014' AND '08/31/14'
GROUP BY taskNotes
HAVING  COUNT(*) > 1
ORDER BY TimesRun desc


SELECT COUNT(*) FROM property

