--INSERT INTO managerassignment (employeeid, assignmentid) VALUES (77,366),(109,366),(123,366),(135,366),(150,366)\

select * from employee where id in (SELECT distinct employeeid from employeejobstatus where jobid IN (SELECT id from assignment where name like '%Manager%' AND propertyid = 8) and endDate > getdate())

select * from managerassignment
;
/**with jobIDList as
(
select id from Assignment where id = 366 
UNION ALL
SELECT Assignment.id from Assignment join jobIDList on ParentAssignmentID = JOBIDList.ID
), employees AS
(select id as employeeID from employee where id in (SELECT distinct employeeid from employeejobstatus where jobid IN (SELECT id from assignment where name like '%Manager%' AND propertyid = 8) and endDate > getdate()))
insert into managerAssignment (employeeID, assignmentID) select employeeID, id from jobIDList join employees on 1 = 1 WHERE employeeID <> 76

**/

SELECT * from managerAssignment

selec

