USE mike_airbags_mobile
GO

/****** Object:  StoredProcedure [dbo].[sp_createDemoScenarioA]    Script Date: 10/29/2012 17:07:51 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sp_createDemoScenarioA]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[sp_createDemoScenarioA]
GO

/****** Object:  StoredProcedure [dbo].[sp_createDemoScenarioA]    Script Date: 10/29/2012 17:07:51 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create proc [dbo].[sp_createDemoScenarioA](@employeeID1 int, @employeeID2 int, @employeeID3 int, @employeeID4 int, @employeeID5 int, @managerEmployeeID int) as
BEGIN
      Declare @assignmentID int
      Declare @jobStatusID1 int
      Declare @jobStatusID2 int
      Declare @jobStatusID3 int
      Declare @jobStatusID4 int
      Declare @jobStatusID5 int

      select @assignmentID = dbo.getHomeJobAssignmentID(@employeeID1, GETDATE())
      select @jobStatusID1 = dbo.getJobStatusID(@employeeID1, @assignmentID, GETDATE())
      select @jobStatusID2 = dbo.getJobStatusID(@employeeID2, @assignmentID, GETDATE())
      select @jobStatusID3 = dbo.getJobStatusID(@employeeID3, @assignmentID, GETDATE())
      select @jobStatusID4 = dbo.getJobStatusID(@employeeID4, @assignmentID, GETDATE())
      select @jobStatusID5 = dbo.getJobStatusID(@employeeID5, @assignmentID, GETDATE())

      -- Changes the open pay period and schedule week to ensure today's date falls within them
      -- Removes all schedules and punches for the 3 employees in the week
      -- Creates schedules for the 3 employees as follows:
      --   Employee1
      --      Schedule for .5 hours back to 7.5 hours ahead with a late in alert
      --      Same schedule for next 4 days
      --      Shift Absence for tomorrow's schedule (removes shift and creates alert)
      --      Shift Drop request for schedule 2 days from now
      --   Employee 2
      --      Schedule for 1 hour from now to 9 hours from now with a coming in soon alert
      --      Same schedule for next 4 days
      --      Shift Tardy Alert for tomorrow's schedule
      --   Employee 3
      --      Schedule for 3 hours back to 5 hours ahead with an approaching break alert
      --      Same schedule for next 4 days
      --      Actual shift with a single in punch for 3 hours back
      --      Shift Swap Request with Employee 2 trading tomorrow's shift with employee 2's shift in same day
      --      Actual Bonus Dollars Earning for the current date
      --   Employee 4
      --      Schedule for 9 hours back to 1 hour back with a late to clock out alert
      --      Actual shift with a single in punch and a break out/back punch set for 9 hours back
      --      Actual Bonus Dollars Earning for the current date
      --   Employee 5
      --      Actual shift with a single in punch from 30 minutes ago with an on clock unscheduled alert

      Declare @PayPeriodStartDate Datetime
      Declare @PayPeriodEndDate Datetime
      Declare @SchedPeriodStartDate Datetime
      Declare @SchedPeriodEndDate Datetime
      Declare @RoundedCurrentTime DateTime
      Declare @PropertyID int
      Declare @employeeShiftID int
      Declare @plannedShiftID int
      Declare @ShiftStartDate datetime
      Declare @roundTo float
      Declare @dropReasonID int
      Declare @tardyReasonID int
      Declare @earliestSchedDate DateTime
      Declare @earliestPayDate DateTime

      exec sp_initDemoParameters @employeeID1, @PropertyID output, @RoundTo output, @PayPeriodStartDate output, @PayPeriodEndDate output, @SchedPeriodStartDate output, @SchedPeriodEndDate output, @RoundedCurrentTime output, @DropReasonID output, @TardyReasonID output, @EarliestSchedDate output, @EarliestPayDate output
    update Assignment set LastPublishedDateTime = getDate(), LastPublishedWeekEndDate = @SchedPeriodEndDate where ID = @assignmentID

      -- Delete old data
      delete from EmployeeShiftRequest
      delete from PortalMessage
      delete from PlannedShift where ShiftDate between @earliestSchedDate and @SchedPeriodEndDate + 5
      delete from EmployeeShift where ShiftTypeCode = 'S' and ShiftDate between @earliestSchedDate and @SchedPeriodEndDate + 5
      delete from EmployeeShift where ShiftTypeCode = 'A' and ShiftDate between @earliestPayDate and @PayPeriodEndDate + 5
      delete from EmployeeEarning where EarningDate between @earliestSchedDate and @SchedPeriodEndDate + 5
      delete from EmployeeAlert

      -- Create new data

      --   Employee1
      --      Schedule for .5 hours back to 7.5 hours ahead with a late in alert
      --      Same schedule for next 4 days
      --      Shift Absence for tomorrow's schedule (removes shift and creates alert)
      --      Shift Drop request for schedule 2 days from now
      set @ShiftStartDate = master.dbo.roundtime(@RoundedCurrentTime - .5 / 24.0, @roundTo)
      exec dbo.sp_createDemoShift 'S', 0, @jobStatusID1, @ShiftStartDate, 8.0, 5, null, @employeeShiftID OUTPUT, @plannedShiftID OUTPUT
      insert into EmployeeAlert (EmployeeID, EmployeeShiftID, PlannedShiftID, AlertType, AlertStartTime, AlertEndTime, extraData) values (@employeeID1, @employeeShiftID, @plannedShiftID, 'LATE_IN', GETDATE(), @ShiftStartDate + 8.0 / 24.0, null)
      delete from EmployeeShift where ID = @employeeShiftID + 1
      insert into EmployeeAlert (EmployeeID, EmployeeShiftID, PlannedShiftID, AlertType, AlertStartTime, AlertEndTime, extraData) values (@employeeID1, null, @plannedShiftID + 1, 'CALLED_IN', GETDATE(), @ShiftStartDate + 32.0 / 24.0 , '{"changeReasonID":' + CONVERT(varchar, @dropReasonID) + ',"comments":"Sorry...can''t make it in today"}')
      set @employeeShiftID = @employeeShiftID + 2
      exec dbo.sp_createDemoShiftDropRequest @employeeShiftID, 'Mother in law coming in to town.', @managerEmployeeID

      --   Employee 2
      --      Schedule for 1 hour from now to 9 hours from now with a coming in soon alert
      --      Same schedule for next 4 days
      --      Shift Tardy Alert for tomorrow's schedule
      set @ShiftStartDate = master.dbo.roundtime(@RoundedCurrentTime + 1.0 / 24.0, @roundTo)
      exec dbo.sp_createDemoShift 'S', 0, @jobStatusID2, @ShiftStartDate, 8.0, 5, null, @employeeShiftID OUTPUT, @plannedShiftID OUTPUT
      insert into EmployeeAlert (EmployeeID, EmployeeShiftID, PlannedShiftID, AlertType, AlertStartTime, AlertEndTime, extraData) values (@employeeID2, @employeeShiftID, @plannedShiftID, 'COMING_IN_SOON', GETDATE(), @ShiftStartDate, null)
      insert into EmployeeAlert (EmployeeID, EmployeeShiftID, PlannedShiftID, AlertType, AlertStartTime, AlertEndTime, extraData) values (@employeeID1, @employeeShiftID + 1, @plannedShiftID + 1, 'CALLED_IN', GETDATE(), @ShiftStartDate + 32.0 / 24.0 , '{"changeReasonID":' + CONVERT(varchar, @tardyReasonID) + ',"comments":"Couldn''t afford alarm software on my IPhone because I blew all my money on an IPad","tardyHours":.45}')

      --   Employee 3
      --      Schedule for 3 hours back to 5 hours ahead with an approaching break alert
      --      Same schedule for next 4 days
      --      Actual shift with a single in punch for 3 hours back
      --      Actual Bonus Dollars Earning for the current date
      set @ShiftStartDate = master.dbo.roundtime(@RoundedCurrentTime - 3.0 / 24.0, @roundTo)
      exec dbo.sp_createDemoShift 'S', 0, @jobStatusID3, @ShiftStartDate, 8.0, 5, null, @employeeShiftID OUTPUT, @plannedShiftID OUTPUT
      insert into EmployeeAlert (EmployeeID, EmployeeShiftID, PlannedShiftID, AlertType, AlertStartTime, AlertEndTime, extraData) values (@employeeID3, @employeeShiftID, @plannedShiftID, 'APPROACHING_BREAK', GETDATE(), master.dbo.roundtime(@ShiftStartDate + 4.5/24.0, @roundTo), '{"breakOutTime":"' + CONVERT(varchar, master.dbo.roundtime(@ShiftStartDate + 4.0/24.0, @roundTo), 126) + '"}')
      exec dbo.sp_createDemoShift 'A', 1, @jobStatusID3, @ShiftStartDate, 8.0, 1, @PayPeriodEndDate, @employeeShiftID OUTPUT, @plannedShiftID OUTPUT
      exec sp_createDemoDollarsEarning 'Bonus', 'BON', 85, @ShiftStartDate, @jobStatusID3

      --   Employee 4
      --      Schedule for 9 hours back to 1 hour back with a late to clock out alert
      --      Actual shift with a single in punch and a break out/back punch set for 9 hours back
      --      Actual Bonus Dollars Earning for the current date
      set @ShiftStartDate = master.dbo.roundtime(@RoundedCurrentTime - 9.0 / 24.0, @roundTo)
      exec dbo.sp_createDemoShift 'S', 0, @jobStatusID4, @ShiftStartDate, 8.0, 1, null, @employeeShiftID OUTPUT, @plannedShiftID OUTPUT
      insert into EmployeeAlert (EmployeeID, EmployeeShiftID, PlannedShiftID, AlertType, AlertStartTime, AlertEndTime, extraData) values (@employeeID3, @employeeShiftID, @plannedShiftID, 'LATE_OUT', @ShiftStartDate + 8.0 / 24.0, @ShiftStartDate + 1.0, null)
      exec dbo.sp_createDemoShift 'A', 3, @jobStatusID4, @ShiftStartDate, 8.0, 1, @PayPeriodEndDate, @employeeShiftID OUTPUT, @plannedShiftID OUTPUT
      exec sp_createDemoDollarsEarning 'Bonus', 'BON', 110, @ShiftStartDate, @jobStatusID4

      --   Employee 5
      --      Actual shift with a single in punch from 30 minutes ago with an on clock unscheduled alert
      set @ShiftStartDate = master.dbo.roundtime(@RoundedCurrentTime - .5 / 24.0, @roundTo)
      exec dbo.sp_createDemoShift 'A', 3, @jobStatusID4, @ShiftStartDate, 8.0, 1, @PayPeriodEndDate, @employeeShiftID OUTPUT, @plannedShiftID OUTPUT
      insert into EmployeeAlert (EmployeeID, EmployeeShiftID, AlertType, AlertStartTime, AlertEndTime, extraData) values (@employeeID5, @employeeShiftID, 'NOT_SCHEDULED', @ShiftStartDate, @ShiftStartDate + 1.0, null)

      -- Show results to user

      select * from EmployeeShift where ShiftTypeCode = 'S' and ShiftDate between @earliestSchedDate and @SchedPeriodEndDate + 5 order by EmployeeID, JobID, ShiftTypeCode, ShiftDate
      select * from EmployeeShift where ShiftTypeCode = 'A' and ShiftDate between @earliestPayDate and @PayPeriodEndDate + 5 order by EmployeeID, JobID, ShiftTypeCode, ShiftDate
      select * from EmployeeShiftPunch where EmployeeShiftID in (select ID from EmployeeShift where ShiftTypeCode = 'A' and ShiftDate between @earliestPayDate and @PayPeriodEndDate + 5) order by EmployeeShiftID, PunchTime
      select * from EmployeeEarning where EarningDate between @earliestPayDate and @PayPeriodEndDate + 5 order by EmployeeID, JobID, EarningDate
      select * from EmployeeAlert
      select * from EmployeeShiftRequest where GiveAwayShiftID in (select ID from EmployeeShift where ShiftTypeCode = 'S' and ShiftDate between @earliestSchedDate and @SchedPeriodEndDate + 5)
      select * from PortalMessage where ShiftID in (select ID from EmployeeShift where ShiftTypeCode = 'S' and ShiftDate between @earliestSchedDate and @SchedPeriodEndDate + 5)
      select ID, LastPublishedDateTime, LastPublishedWeekEndDate from Assignment where ID = @assignmentID
END

GO


