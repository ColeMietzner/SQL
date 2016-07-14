if exists 
	(select ID from BudgetStaffStat where JobclassID = 2523 and StatTypeCode = 'O' and StatDate = '2012-06-23')    
	
update BudgetStaffStat 
set 
	OriginalHours = 0.0, StaffHours = 0.0 + isnull(bj.HoursAmount,0.0),
	OriginalRate = ISNULL(OriginalRate,0.0),    
	StaffRate = ISNULL(StaffRate,0.0 + bj.RateAmount),
	OTRate = ISNULL(OTRate,0.0 * 0.5),    
	StaffTotal = (0.0 + bj.HoursAmount) * ISNULL(StaffRate,0.0 + bj.RateAmount) + OTHours * ISNULL(OTRate,0.0 * 0.5)    
from
	 BudgetStaffStat bs    
left join (select JobclassID,StatTypeCode,StatDate,SUM(case when AdjustmentType = 'HRS' then Amount else 0.0 end) as HoursAmount,SUM
			(case when AdjustmentType = 'RAT' then Amount else 0.0 end) as RateAmount               
from BudgetJobclassAdjustment where JobclassID = 2523 and StatTypeCode = 'O' and StatDate = '2012-06-23'               
group by JobclassID,StatTypeCode,StatDate) bj on bj.JobclassID = bs.JobclassID and bj.StatTypeCode = bs.StatTypeCode and bj.StatDate = bs.StatDate    
where bs.JobclassID = 2523 and bs.StatTypeCode = 'O' and bs.StatDate = '2012-06-23' 

else   

insert BudgetStaffStat (JobclassID,StatTypeCode,StatDate,OriginalHours,StaffHours,OriginalRate,StaffRate,OTHours,OTRate,StaffTotal)   
values (2523,'O','2012-06-23',0.0,0.0,0.0,0.0,0.0,0.0 * 0.5,0.0 * 0.0);  

if exists 
(select ID from BudgetStaffStat where JobclassID = 2523 and StatTypeCode = 'O' and StatDate = '2012-06-24')    
	update BudgetStaffStat set OriginalHours = 0.0, StaffHours = 0.0 + isnull(bj.HoursAmount,0.0),OriginalRate = ISNULL(OriginalRate,0.0),    
StaffRate = ISNULL(StaffRate,0.0 + bj.RateAmount),OTRate = ISNULL(OTRate,0.0 * 0.5),    
StaffTotal = (0.0 + bj.HoursAmount) * ISNULL(StaffRate,0.0 + bj.RateAmount) + OTHours * ISNULL(OTRate,0.0 * 0.5)    
from BudgetStaffStat bs    
left join (select JobclassID,StatTypeCode,StatDate,SUM(case when AdjustmentType = 'HRS' then Amount else 0.0 end) as HoursAmount,
SUM(case when AdjustmentType = 'RAT' then Amount else 0.0 end) as RateAmount               
from BudgetJobclassAdjustment where JobclassID = 2523 and StatTypeCode = 'O' and StatDate = '2012-06-24'               
group by JobclassID,StatTypeCode,StatDate) bj on bj.JobclassID = bs.JobclassID and bj.StatTypeCode = bs.StatTypeCode and bj.StatDate = bs.StatDate    
where bs.JobclassID = 2523 and bs.StatTypeCode = 'O' and bs.StatDate = '2012-06-24' 
else insert BudgetStaffStat 
(JobclassID,StatTypeCode,StatDate,OriginalHours,StaffHours,OriginalRate,StaffRate,OTHours,OTRate,StaffTotal)    
values (2523,'O','2012-06-24',0.0,0.0,0.0,0.0,0.0,0.0 * 0.5,0.0 * 0.0);  

if exists (select ID from BudgetStaffStat where JobclassID = 2523 and StatTypeCode = 'O' and StatDate = '2012-06-25')    
update BudgetStaffStat set OriginalHours = 0.0, StaffHours = 0.0 + isnull(bj.HoursAmount,0.0),OriginalRate = ISNULL(OriginalRate,0.0),    
StaffRate = ISNULL(StaffRate,0.0 + bj.RateAmount),OTRate = ISNULL(OTRate,0.0 * 0.5),    
StaffTotal = (0.0 + bj.HoursAmount) * ISNULL(StaffRate,0.0 + bj.RateAmount) + OTHours * ISNULL(OTRate,0.0 * 0.5)    
from BudgetStaffStat bs   
left join (select JobclassID,StatTypeCode,StatDate,
SUM(case when AdjustmentType = 'HRS' then Amount else 0.0 end) as HoursAmount,SUM(case when AdjustmentType = 'RAT' then Amount else 0.0 end) 
as RateAmount              
from BudgetJobclassAdjustment where JobclassID = 2523 and StatTypeCode = 'O' and StatDate = '2012-06-25'               
group by JobclassID,StatTypeCode,StatDate) bj on bj.JobclassID = 
bs.JobclassID and bj.StatTypeCode = bs.StatTypeCode and bj.StatDate = bs.StatDate    
where bs.JobclassID = 2523 and bs.StatTypeCode = 'O' and bs.StatDate = '2012-06-25' 
else insert BudgetStaffStat 
(JobclassID,StatTypeCode,StatDate,OriginalHours,StaffHours,OriginalRate,StaffRate,OTHours,OTRate,StaffTotal)    
values (2523,'O','2012-06-25',0.0,0.0,0.0,0.0,0.0,0.0 * 0.5,0.0 * 0.0);  
if exists (select ID from BudgetStaffStat where JobclassID = 2523 and StatTypeCode = 'O' and StatDate = '2012-06-26')    
update BudgetStaffStat set OriginalHours = 0.0, StaffHours = 0.0 + isnull(bj.HoursAmount,0.0),OriginalRate = ISNULL(OriginalRate,0.0),    
StaffRate = ISNULL(StaffRate,0.0 + bj.RateAmount),OTRate = ISNULL(OTRate,0.0 * 0.5),    
StaffTotal = (0.0 + bj.HoursAmount) * ISNULL(StaffRate,0.0 + bj.RateAmount) + OTHours * ISNULL(OTRate,0.0 * 0.5)    
from BudgetStaffStat bs    
left join (select JobclassID,StatTypeCode,StatDate,SUM(case when AdjustmentType = 'HRS' then Amount else 0.0 end) as HoursAmount,
SUM(case when AdjustmentType = 'RAT' then Amount else 0.0 end) as RateAmount               
from BudgetJobclassAdjustment where JobclassID = 2523 and StatTypeCode = 'O' and StatDate = '2012-06-26'               
group by JobclassID,StatTypeCode,StatDate) bj on bj.JobclassID = bs.JobclassID and bj.StatTypeCode = bs.StatTypeCode and bj.StatDate = bs.StatDate    
where bs.JobclassID = 2523 and bs.StatTypeCode = 'O' and bs.StatDate = '2012-06-26' 

else    insert BudgetStaffStat (JobclassID,StatTypeCode,StatDate,OriginalHours,StaffHours,OriginalRate,StaffRate,OTHours,OTRate,StaffTotal)    
values (2523,'O','2012-06-26',0.0,0.0,0.0,0.0,0.0,0.0 * 0.5,0.0 * 0.0);  

if exists (select ID from 
BudgetStaffStat where JobclassID = 2523 and StatTypeCode = 'O' and StatDate = '2012-06-27')    
update BudgetStaffStat set OriginalHours = 0.0, StaffHours = 0.0 + isnull(bj.HoursAmount,0.0),OriginalRate = ISNULL(OriginalRate,0.0),    
StaffRate = ISNULL(StaffRate,0.0 + bj.RateAmount),OTRate = ISNULL(OTRate,0.0 * 0.5),    
StaffTotal = (0.0 + bj.HoursAmount) * ISNULL(StaffRate,0.0 + bj.RateAmount) + OTHours * ISNULL(OTRate,0.0 * 0.5)    
from BudgetStaffStat bs    
left join (select JobclassID,StatTypeCode,StatDate,SUM(case when AdjustmentType = 'HRS' then Amount else 0.0 end) as HoursAmount,
SUM(case when AdjustmentType = 'RAT' then Amount else 0.0 end) as RateAmount               
from BudgetJobclassAdjustment where JobclassID = 2523 and StatTypeCode = 'O' and StatDate = '2012-06-27'               
group by JobclassID,StatTypeCode,StatDate) bj on bj.JobclassID = bs.JobclassID and bj.StatTypeCode = bs.StatTypeCode and bj.StatDate = bs.StatDate    
where bs.JobclassID = 2523 and bs.StatTypeCode = 'O' and bs.StatDate = '2012-06-27' 
else    insert BudgetStaffStat (JobclassID,StatTypeCode,StatDate,OriginalHours,StaffHours,OriginalRate,StaffRate,OTHours,OTRate,StaffTotal)    
values (2523,'O','2012-06-27',0.0,0.0,0.0,0.0,0.0,0.0 * 0.5,0.0 * 0.0);  
if exists (select ID from BudgetStaffStat where JobclassID = 2523 and StatTypeCode = 'O' and StatDate = '2012-06-28')    
update BudgetStaffStat set OriginalHours = 0.0, StaffHours = 0.0 + isnull(bj.HoursAmount,0.0),OriginalRate = ISNULL(OriginalRate,0.0),    
StaffRate = ISNULL(StaffRate,0.0 + bj.RateAmount),OTRate = ISNULL(OTRate,0.0 * 0.5),    
StaffTotal = (0.0 + bj.HoursAmount) * ISNULL(StaffRate,0.0 + bj.RateAmount) + OTHours * ISNULL(OTRate,0.0 * 0.5)    
from BudgetStaffStat bs    
left join (select JobclassID,StatTypeCode,StatDate,SUM(case when AdjustmentType = 'HRS' then Amount else 0.0 end) as HoursAmount,
SUM(case when AdjustmentType = 'RAT' then Amount else 0.0 end) as RateAmount              
from BudgetJobclassAdjustment where JobclassID = 2523 and StatTypeCode = 'O' and StatDate = '2012-06-28'              
group by JobclassID,StatTypeCode,StatDate) bj on bj.JobclassID = bs.JobclassID and bj.StatTypeCode = bs.StatTypeCode and bj.StatDate = bs.StatDate    
where bs.JobclassID = 2523 and bs.StatTypeCode = 'O' and bs.StatDate = '2012-06-28' 
else    insert BudgetStaffStat (JobclassID,StatTypeCode,StatDate,OriginalHours,StaffHours,OriginalRate,StaffRate,OTHours,OTRate,StaffTotal)    
values (2523,'O','2012-06-28',0.0,0.0,0.0,0.0,0.0,0.0 * 0.5,0.0 * 0.0);
