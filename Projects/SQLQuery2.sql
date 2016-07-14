select * from employeeTimeOff
select * from TORDistribution where TimeOffRequestID = 8559

Select * from EmployeeEarning where Rate is Null and TORDistributionID is not null order by EarningDate, SkillID

Select * from EmployeeEarning where Rate is not Null and TORDistributionID is not null order by EarningDate, SkillID


with test as 
(select SkillID, EarningDate from EmployeeEarning where TORDistributionID is not null group by SkillID, EarningDate having COUNT(*) > 1) 

select EmployeeEarning.* from EmployeeEarning join test on EmployeeEarning.SkillID = test.SkillID and EmployeeEarning.EarningDate = test.EarningDate where EmployeeEarning.TORDistributionID is not null

order by EmployeeEarning.SkillID, EmployeeEarning.EarningDate 
go


select * from TORDistribution where ID in (3707, 3715, 3721)
select * from EmployeeTimeOff where ID in (select TimeOffRequestID from TORDistribution where ID in (ABOVE QUERY HERE)) and Status not in ('C','D','P')

select * from EmployeeTimeOff where ID in (select TimeOffRequestID from TORDistribution where ID in (3707,3715,3721)) and Status not in ('C','D','P')