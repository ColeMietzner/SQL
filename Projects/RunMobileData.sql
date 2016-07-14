-- Example usage (redlionel)
-- exec sp_createDemoScenarioA 2597, 2646, 2670, 2598, 2655, 2610

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[getHomeJobAssignmentID]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
                DROP FUNCTION dbo.getHomeJobAssignmentID
GO
create function dbo.getHomeJobAssignmentID (@EmployeeID int, @AsOfDate datetime) RETURNS int AS
BEGIN
                return (select JobID from EmployeeJobStatus where EmployeeID = @EmployeeID and Home = 'Y' and @AsOfDate between StartDate and EndDate)
END
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[getJobStatusID]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
                DROP FUNCTION dbo.getJobStatusID
GO
create function dbo.getJobStatusID (@EmployeeID int, @JobID int, @AsOfDate datetime) RETURNS int AS
BEGIN
                return (select ID from EmployeeJobStatus where EmployeeID = @EmployeeID and JobID = @JobID and @AsOfDate between StartDate and EndDate)
END
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[getBadgeNo]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
                DROP FUNCTION dbo.getBadgeNo
GO
create function dbo.getBadgeNo (@EmployeeID int, @JobID int) RETURNS varchar(25) AS
BEGIN
                Declare @badgeNo varchar(25)
                select @badgeNo = BadgeNo from EmployeeJobBadge where EmployeeID = @EmployeeID and JobID = @JobID

                if @badgeNo is null
                                select @badgeNo = BadgeNo from EmployeeJobBadge where EmployeeID = @EmployeeID

                return @badgeNo
END
GO

if exists (select * from sys.objects where object_id = OBJECT_ID('sp_createDemoShiftChangeReasonIfMissing') AND type in ('P', 'PC'))
                drop proc sp_createDemoShiftChangeReasonIfMissing
go
create proc sp_createDemoShiftChangeReasonIfMissing(@changeReasonName varchar(50), @ChangeReasonType char(1), @propertyID int, @changeReasonID int OUTPUT) as
BEGIN
                select @changeReasonID = id from ShiftChangeReason where PropertyID = @propertyID and Name = @changeReasonName
                if @changeReasonID is null
                BEGIN
                                insert into ShiftChangeReason (PropertyID, Name, ChangeReasonType) values (@propertyID, @changeReasonName, @ChangeReasonType)
                                set @changeReasonID = SCOPE_IDENTITY()
                END
END
go

if exists (select * from sys.objects where object_id = OBJECT_ID('sp_createDemoEarningCategoryIfMissing') AND type in ('P', 'PC'))
                drop proc sp_createDemoEarningCategoryIfMissing
go
create proc sp_createDemoEarningCategoryIfMissing(@earningCategoryName varchar(50), @UOM char(1), @propertyID int, @earningCategoryID int OUTPUT) as
BEGIN
                select @earningCategoryID = id from EarningCategory where PropertyID = @propertyID and Name = @earningCategoryName
                if @earningCategoryID is null
                BEGIN
                                insert into EarningCategory (PropertyID, Name, UOM) values (@propertyID, @earningCategoryName, @UOM)
                                set @earningCategoryID = SCOPE_IDENTITY()
                END
END
go

if exists (select * from sys.objects where object_id = OBJECT_ID('sp_createDemoEarningIfMissing') AND type in ('P', 'PC'))
                drop proc sp_createDemoEarningIfMissing
go
create proc sp_createDemoEarningIfMissing(@earningName varchar(50), @processorCode varchar(10), @UOM char(1), @type char(1), @propertyID int, @earningTypeID int OUTPUT) as
BEGIN
                Declare @earningCategoryID int
                Declare @categoryName varchar(50)

                select @earningTypeID = id from EarningType where PropertyID = @propertyID and Name = @earningName
                if @earningTypeID is null
                BEGIN
                                set @categoryName = 'Other (' + @UOM + ')'
                                exec sp_createDemoEarningCategoryIfMissing @categoryName, @UOM, @propertyID, @earningCategoryID output
                                insert into EarningType (PropertyID, Name, ProcessorCode, UOM, Type, EarningCategoryID, IncludeInBankedHours, IncludeInWorkedHours, IncludeInSalaryDistribution, AccrualFrequency, IncludeInReallocation) values (@propertyID, @earningName, @processorCode, @UOM, 'E', @earningCategoryID, 'N', case when @UOM = 'H' then 'Y' else 'N' end, case when @UOM = 'H' then 'Y' else 'N' end, 'P', 'N')
                                select @earningTypeID = SCOPE_IDENTITY()
                END
END
go

if exists (select * from sys.objects where object_id = OBJECT_ID('sp_createDemoDollarsEarning') AND type in ('P', 'PC'))
                drop proc sp_createDemoDollarsEarning
go
create proc sp_createDemoDollarsEarning (@earningName varchar(50), @processorCode varchar(10), @amount decimal(9,2), @earningDate datetime, @JobstatusID int) as
BEGIN
                declare @earningTypeID int
                declare @propertyID int
                declare @employeeID int
                declare @jobID int

                select @EmployeeID = EmployeeID, @JobID = JobID from EmployeeJobStatus where ID = @JobstatusID
                select @propertyID = PropertyID from Employee where ID = @EmployeeID
                exec sp_createDemoEarningIfMissing @earningName, @processorCode, 'D', 'E', @propertyID, @earningTypeID output
                set @earningDate = convert(datetime, convert(varchar, @earningDate, 100))
                delete from EmployeeEarning where EarningDate = @earningDate and Dollars = @amount and EmployeeID = @employeeID and JobID = @JobID and EarningTypeID = @earningTypeID
                insert into EmployeeEarning (JobID, EmployeeID, EarningTypeID, EarningDate, Source, Dollars, TotalDollars, PayDate) values (@jobID, @EmployeeID, @earningTypeID, @earningDate, 'M', @amount, @amount, @earningDate)
END
go


if exists (select * from sys.objects where object_id = OBJECT_ID('sp_adjustPeriodDates') AND type in ('P', 'PC'))
                drop proc sp_adjustPeriodDates
go
create proc sp_adjustPeriodDates(@currentDate datetime, @PropertyID int, @ppStartDate DateTime output) as
BEGIN
                Declare @ppEndDate datetime
                Declare @PayPeriodTypeCode varchar(10)
                Declare @PeriodEndDate datetime
                Declare @ChangePPEndDate tinyint
                select @ppEndDate = CurrentPPEndDate, @PayPeriodTypeCode = PayPeriodTypeCode, @PeriodEndDate = PeriodEndDate from Property where ID = @PropertyID

    set @ChangePPEndDate = case when @ppEndDate < @currentDate then 1 else 0 end

                if @PayPeriodTypeCode = 'BW'
                BEGIN
                                if @ppEndDate < @currentDate
                                                set @ppEndDate = DateAdd(DAY, (DATEDIFF(DAY, @ppEndDate, @currentDate) / 14 + 1) * 14,@ppEndDate)

                                set @ppStartDate = @ppEndDate - 13.0
                END
                else
                BEGIN
                                if @PayPeriodTypeCode = 'WK'
                                BEGIN
                                                if @ppEndDate < @currentDate
                                                                set @ppEndDate = DateAdd(DAY, (DATEDIFF(DAY, @ppEndDate, @currentDate) / 7 + 1) * 7,@ppEndDate)
                                                set @ppStartDate = @ppEndDate - 6
                                END
                                else
                                BEGIN
                                                if @PayPeriodTypeCode = 'SM'
                                                BEGIN
                                                                Declare @StartDay int
                                                                Declare @EndDay int
                                                                if DATEPART(DAY, @currentDate) < 16
                                                                BEGIN
                                                                                set @ppStartDate = @currentDate + 16 - DATEPART(DAY, @currentDate)
                                                                                set @ppEndDate = DATEADD(MONTH, 1, @currentDate + 1 - DATEPART(DAY, @currentDate)) - 1
                                                                END
                                                                else
                                                                BEGIN
                                                                                set @ppStartDate = @currentDate + 1 - DATEPART(DAY, @currentDate)
                                                                                set @ppEndDate = @currentDate + 14
                                                                END
                                                END
                                                else
                                                BEGIN
                                                                set @ppStartDate = @currentDate + 1 - DATEPART(DAY, @currentDate)
                                                                set @ppEndDate = DATEADD(MONTH, 1, @currentDate + 1 - DATEPART(DAY, @currentDate)) - 1
                                                END
                                END
                END
                if @ChangePPEndDate = 1
                                update Property set CurrentPPEndDate = @ppEndDate where ID = @PropertyID

                if @PeriodEndDate <= @currentDate
                BEGIN
                                Declare @SchedStartDate datetime
                                Declare @SchedEndDate datetime

                                set @SchedEndDate = DateAdd(DAY, (DATEDIFF(DAY, @PeriodEndDate, @currentDate) / 7 + 1) * 7,@PeriodEndDate)
                                set @SchedStartDate = @SchedEndDate - 6

                                update Property set PeriodStartDate = @SchedStartDate, PeriodEndDate = @SchedEndDate where ID = @PropertyID
                END
END
GO

if exists (select * from sys.objects where object_id = OBJECT_ID('sp_initDemoParameters') AND type in ('P', 'PC'))
                drop proc sp_initDemoParameters
go
create proc sp_initDemoParameters(@ForEmployeeID int, @PropertyID int output, @RoundTo float output, @PayPeriodStartDate datetime output, @PayPeriodEndDate datetime output, @SchedPeriodStartDate datetime output, @SchedPeriodEndDate datetime output, @RoundedCurrentTime datetime output, @DropReasonID int output, @TardyReasonID int output, @EarliestSchedDate datetime output, @EarliestPayDate datetime output) as
BEGIN
                Declare @CurrentDate Date

                select @PropertyID = PropertyID from Employee where ID = @ForEmployeeID
                select @RoundTo = periodLength / 60.0 from Property where ID = @PropertyID
                set @CurrentDate = getDate()
                exec sp_adjustPeriodDates @currentDate, @PropertyID, @PayPeriodStartDate OUTPUT
                select @SchedPeriodStartDate = PeriodStartDate, @SchedPeriodEndDate = PeriodEndDate, @PayPeriodEndDate = CurrentPPEndDate from Property where ID = @PropertyID
                set @RoundedCurrentTime = master.dbo.roundTime(GETDATE() + @roundTo/24.0, @roundTo)
                select @dropReasonID = min(ID) from ShiftChangeReason where PropertyID = @PropertyID and ChangeReasonType = 'A'
                select @tardyReasonID = min(ID) from ShiftChangeReason where PropertyID = @PropertyID and ChangeReasonType = 'T'

                -- Delete all planned shift ID's and Portal Messages linked to the scheduled shifts that are about to be deleted

                set @earliestSchedDate = case when @CurrentDate < @SchedPeriodStartDate then @CurrentDate else @SchedPeriodStartDate end
                set @earliestPayDate = case when @CurrentDate < @PayPeriodStartDate then @CurrentDate else @PayPeriodStartDate end
END
go

if exists (select * from sys.objects where object_id = OBJECT_ID('sp_createDemoPortalMessage') AND type in ('P', 'PC'))
                drop proc sp_createDemoPortalMessage
go
create proc sp_createDemoPortalMessage(@fromEmployeeID int, @toEmployeeID int, @subject varchar(255), @body varchar(MAX), @shiftID int) as
BEGIN
                Declare @messageID int
                insert into PortalMessage (SentByEmployeeID, Subject, Message, Important, DateTimeSent, DateTimeExpires, MessageType, ShiftID) values (@fromEmployeeID, @subject, @body, 'N', getDate(), getDate() + 30, 'M', @shiftID)
                set @messageID = SCOPE_IDENTITY()
                insert into PortalMessageRecipient (PortalMessageID, EmployeeID, MessageRead, Pinned) values (@messageID, @toEmployeeID, 'N', 'N')
END
go

if exists (select * from sys.objects where object_id = OBJECT_ID('sp_createDemoShiftDropRequest') AND type in ('P', 'PC'))
                drop proc sp_createDemoShiftDropRequest
go
create proc sp_createDemoShiftDropRequest(@dropShiftID int, @comments varchar(1024), @managerEmployeeID int) as
BEGIN
                Declare @owningEmployeeID int
                Declare @plannedShiftID int
                Declare @owningEmployeeName varchar(255)
                Declare @body varchar(1024)
                Declare @jobName varchar(50)
                Declare @JobStatusID int
                Declare @JobID int
                Declare @shiftStartDate varchar(50)
                Declare @propertyID int
                Declare @shiftChangeReasonID int

                select @shiftStartDate = CONVERT(varchar, StartTime, 100), @JobStatusID = dbo.getJobStatusID(employeeID, JobID, ShiftDate), @PlannedShiftID = plannedShiftID, @JobID = JobID, @owningEmployeeID = EmployeeID from EmployeeShift where ID = @dropShiftID
                select @propertyID = propertyID, @jobName = Name from Assignment where ID = @JobID
                select @owningEmployeeName = name from Employee where ID = @owningEmployeeID
                select @body = 'A shift drop request for the ' + @jobName + ' shift on ' + @shiftStartDate + ' was initiated by ' + @owningEmployeeName + '.
                Comments: ' + @comments

                exec sp_createDemoPortalMessage @owningEmployeeID, @managerEmployeeID, 'Shift Drop Request Initiated', @body, @dropShiftID
                exec sp_createDemoShiftChangeReasonIfMissing 'Drop', 'D', @propertyID, @shiftChangeReasonID output

                insert into EmployeeShiftRequest
                                (RequestedByID, EnteredOn, GiveAwayShiftID, EmployeeComments, EMailSent, [Status], ChangeReasonID, PlannedShiftID)
                values
                                (@owningEmployeeID, GETDATE(), @dropShiftID, @comments, 'N', 'P', @shiftChangeReasonID, @PlannedShiftID)
END
go

if exists (select * from sys.objects where object_id = OBJECT_ID('sp_createDemoShift') AND type in ('P', 'PC'))
                drop proc sp_createDemoShift
go
create proc sp_createDemoShift (@ShiftType char(1) = 'S', @NumberActualPunches int = 4, @JobStatusID int, @startDateTime datetime, @duration float, @numberCopies int, @latestDate datetime, @employeeShiftID int OUTPUT, @PlannedShiftID int OUTPUT) as
BEGIN
                Declare @startDate Date
                Declare @endDateTime DateTime
                Declare @shiftCategoryID int
                Declare @roundTo float
                Declare @PropertyID int
                Declare @employeeID int
                Declare @jobID int

    select @employeeID = EmployeeID, @jobID = JobID from EmployeeJobStatus where ID = @JobStatusID
    select @PropertyID = PropertyID, @employeeID = id from Employee where ID = @employeeID
    select @roundTo = periodLength / 60.0 from Property where ID = @PropertyID

                set @startDate = @startDateTime
                set @endDateTime = master.dbo.roundtime(@startDateTime + @duration / 24.0, @roundTo)
                select top 1 @ShiftCategoryID = ID from ShiftCategory where PropertyID = @PropertyID and IncludeInProductivity = 'Y' and TrainingShift = 'N'

                if @ShiftType = 'S'
                BEGIN
                                insert into PlannedShift
                                                (JobID, ShiftType, ShiftDate, StartDateTime, EndDateTime, Duration, Source)
                                values
                                                (@jobID, 'F', @startDate, @startDateTime, @endDateTime, @duration, 'M')
                END

                set @PlannedShiftID = SCOPE_IDENTITY()

                insert into EmployeeShift
                                (EmployeeID, JobID, ShiftDate, ShiftTypeCode, ShiftCategoryID, [Source], CreatedOnDT, WorkedHours, AdjHours, NetHours, AvgRate, NetDollars, RegHours,
                                RegRate, RegDollars, AutoOTHours, AdjOTHours, OTHours, OTRate, OTDollars, AutoDTHours, AdjDTHours, DTHours, DTRate, DTDollars, StartTime,
                                EndTime, BankedHours, PlannedShiftID, Overlap)
                values
                                (@employeeID, @jobID, @startDate, @ShiftType, @shiftCategoryID, 'M', GETDATE(), @duration, 0.0, @duration,
                                case when @ShiftType = 'S' then 0.0 else 10.0 end, case when @ShiftType = 'S' then 0.0 else 10.0 * @duration end, @duration,
                                case when @ShiftType = 'S' then 0.0 else 10.0 end, case when @ShiftType = 'S' then 0.0 else @duration * 10.0 end, 0.0, 0.0, 0.0, 0.0,
                                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, @startDateTime, @endDateTime, 0.0, @PlannedShiftID, 'N')

                set @EmployeeShiftID = SCOPE_IDENTITY()

                if @ShiftType = 'A'
                BEGIN
                                Declare @BadgeNo varchar(25)
                                Declare @inPunch datetime
                                Declare @breakPunch datetime
                                Declare @backPunch datetime
                                Declare @outPunch datetime

                                set @inPunch = @startDate
                                set @breakPunch = master.dbo.roundtime(@startDateTime + 4.0/24.0, @roundTo)
                                set @backPunch = master.dbo.roundtime(@startDateTime + 4.5/24.0, @roundTo)
                                set @outPunch = master.dbo.roundtime(@startDateTime + @duration / 24.0, @roundTo)

                                select @BadgeNo = dbo.getBadgeNo(@employeeID, @jobID)

                                if @NumberActualPunches > 0 and @BadgeNo is not null
                                                insert into EmployeeShiftPunch (EmployeeShiftID, PunchTypeCode, RawPunch, PunchTime, AdjTime, RoundedTime, BadgeNo, Source) values (@employeeShiftID, 'IN', 'Hand', @inPunch, @inPunch, @inPunch, @BadgeNo, 'M')

                                if @NumberActualPunches > 3 and @BadgeNo is not null
                                                insert into EmployeeShiftPunch (EmployeeShiftID, PunchTypeCode, RawPunch, PunchTime, AdjTime, RoundedTime, BadgeNo, Source) values (@employeeShiftID, 'OUT', 'Hand', @outPunch, @outPunch, @outPunch, @BadgeNo, 'M')

                                if @duration >= 5
                                BEGIN
                                                if @NumberActualPunches > 1 and @BadgeNo is not null
                                                                insert into EmployeeShiftPunch (EmployeeShiftID, PunchTypeCode, RawPunch, PunchTime, AdjTime, RoundedTime, BadgeNo, Source) values (@employeeShiftID, 'BREAK', 'Hand', @breakPunch, @breakPunch, @breakPunch, @BadgeNo, 'M')

                                                if @NumberActualPunches > 2 and @BadgeNo is not null
                                                                insert into EmployeeShiftPunch (EmployeeShiftID, PunchTypeCode, RawPunch, PunchTime, AdjTime, RoundedTime, BadgeNo, Source) values (@employeeShiftID, 'BACK', 'Hand', @backPunch, @backPunch, @backPunch, @BadgeNo, 'M')
                                END

                END

    if @latestDate is null
       set @latestDate = @startDateTime + 2.0 + @numberCopies


                if @numberCopies > 1 and convert(date, @startDateTime + 1.0) <= @latestDate
                BEGIN
                                set @startDateTime = @startDateTime + 1.0
                                set @numberCopies = @numberCopies - 1
                                exec sp_createDemoShift @ShiftType, @NumberActualPunches, @JobStatusID, @startDateTime, @duration, @numberCopies, @latestDate, @employeeShiftID, @plannedShiftID
                END
END
GO

if exists (select * from sys.objects where object_id = OBJECT_ID('sp_createDemoScenarioA') AND type in ('P', 'PC'))
                drop proc sp_createDemoScenarioA
go
create proc sp_createDemoScenarioA(@employeeID1 int, @employeeID2 int, @employeeID3 int, @employeeID4 int, @employeeID5 int, @managerEmployeeID int) as
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
-- CREATE IN MASTER
use master
go
if exists (select * from sys.objects where object_id = OBJECT_ID('dbo.RoundTime') AND type in ('FN', 'IF', 'TF', 'FS', 'FT'))
                drop function dbo.RoundTime
go
CREATE function [dbo].[RoundTime] (@Time datetime, @RoundTo float) returns datetime as
begin
                declare @RoundedTime smalldatetime
                declare @Multiplier float
                set @Multiplier= 24.0/@RoundTo
                set @RoundedTime = ROUND(cast(cast(convert(varchar,@Time,121) as datetime) as float) * @Multiplier,0)/@Multiplier
                return @RoundedTime
end
go
