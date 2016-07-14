CREATE VIEW dbo.reopenedCount as

WITH items as 
(
select 
	cgroup.ID as CID, cgroup.issueid, cgroup.AUTHOR, item.*, row_number() OVER (partition by issueID ORDER BY item.ID) AS IssueOrder 
from 
	changegroup cgroup JOIN changeitem item on (item.groupid = cgroup.id)
where 
	issueid in 
	(
		SELECT id from jiraissue 
		where datepart(quarter,UPDATED) = datepart(quarter,getdate()) and datepart(year,UPDATED) = datepart(year,getdate()) and PROJECT = 10000
	)
	and FIELD like 'status'
	and (item.NEWSTRING like 'REOPENED' OR item.NEWSTRING LIKE 'development completed')
)
, reopenquery AS
(SELECT devcomp.* FROM items reopens join items devcomp ON reopens.issueid = devcomp.ISSUEid AND reopens.issueorder - 1 = devcomp.issueorder where reopens.newstring LIKE 'reopened' AND devcomp.newstring LIKE 'development completed')
, reopens as
(SELECT Author, count(*) as reopenedIssues, sum(issueorder) as weight FROM reopenquery group by AUthor)
select * from reopens 

sp_reopen_reason_crosstab '9/1/12', '12/31/12'
