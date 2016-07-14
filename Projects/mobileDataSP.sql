use mike_airbags_mobile
/****** Object:  StoredProcedure [dbo].[sp_adjustPeriodDates]    Script Date: 10/29/2012 17:28:32 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sp_adjustPeriodDates]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[sp_adjustPeriodDates]
GO

/****** Object:  StoredProcedure [dbo].[sp_adjustPeriodDates]    Script Date: 10/29/2012 17:28:32 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create proc [dbo].[sp_adjustPeriodDates](@currentDate datetime, @PropertyID int, @ppStartDate DateTime output) as
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
/****** Object:  StoredProcedure [dbo].[sp_initDemoParameters]    Script Date: 10/29/2012 17:24:09 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sp_initDemoParameters]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[sp_initDemoParameters]
GO


/****** Object:  StoredProcedure [dbo].[sp_initDemoParameters]    Script Date: 10/29/2012 17:24:09 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create proc [dbo].[sp_initDemoParameters](@ForEmployeeID int, @PropertyID int output, @RoundTo float output, @PayPeriodStartDate datetime output, @PayPeriodEndDate datetime output, @SchedPeriodStartDate datetime output, @SchedPeriodEndDate datetime output, @RoundedCurrentTime datetime output, @DropReasonID int output, @TardyReasonID int output, @EarliestSchedDate datetime output, @EarliestPayDate datetime output) as
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

GO




/****** Object:  StoredProcedure [dbo].[sp_createDemoShift]    Script Date: 10/29/2012 17:24:25 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sp_createDemoShift]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[sp_createDemoShift]
GO


/****** Object:  StoredProcedure [dbo].[sp_createDemoShift]    Script Date: 10/29/2012 17:24:25 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create proc [dbo].[sp_createDemoShift] (@ShiftType char(1) = 'S', @NumberActualPunches int = 4, @JobStatusID int, @startDateTime datetime, @duration float, @numberCopies int, @latestDate datetime, @employeeShiftID int OUTPUT, @PlannedShiftID int OUTPUT) as
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



/****** Object:  StoredProcedure [dbo].[sp_createDemoShiftDropRequest]    Script Date: 10/29/2012 17:24:39 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sp_createDemoShiftDropRequest]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[sp_createDemoShiftDropRequest]
GO

/****** Object:  StoredProcedure [dbo].[sp_createDemoShiftDropRequest]    Script Date: 10/29/2012 17:24:39 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create proc [dbo].[sp_createDemoShiftDropRequest](@dropShiftID int, @comments varchar(1024), @managerEmployeeID int) as
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

GO


/****** Object:  StoredProcedure [dbo].[sp_createDemoDollarsEarning]    Script Date: 10/29/2012 17:24:54 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sp_createDemoDollarsEarning]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[sp_createDemoDollarsEarning]
GO

/****** Object:  StoredProcedure [dbo].[sp_createDemoDollarsEarning]    Script Date: 10/29/2012 17:24:54 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create proc [dbo].[sp_createDemoDollarsEarning] (@earningName varchar(50), @processorCode varchar(10), @amount decimal(9,2), @earningDate datetime, @JobstatusID int) as
BEGIN
      declare @earningTypeID int
      declare @propertyID int
      declare @employeeID int
      declare @jobID int

      select @EmployeeID = EmployeeID, @JobID = JobID from EmployeeJobStatus where ID = @JobstatusID
      select @propertyID = PropertyID from Employee where ID = @EmployeeID
      exec sp_createDemoEarningCategoryIfMissing @earningName, @processorCode, 'D', 'E', @propertyID, @earningTypeID output
      set @earningDate = convert(datetime, convert(varchar, @earningDate, 100))
      delete from EmployeeEarning where EarningDate = @earningDate and Dollars = @amount and EmployeeID = @employeeID and JobID = @JobID and EarningTypeID = @earningTypeID
      insert into EmployeeEarning (JobID, EmployeeID, EarningTypeID, EarningDate, Source, Dollars, TotalDollars, PayDate) values (@jobID, @EmployeeID, @earningTypeID, @earningDate, 'M', @amount, @amount, @earningDate)
END

GO





/****** Object:  StoredProcedure [dbo].[sp_createDemoShiftChangeReasonIfMissing]    Script Date: 10/29/2012 17:29:05 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sp_createDemoShiftChangeReasonIfMissing]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[sp_createDemoShiftChangeReasonIfMissing]
GO

/****** Object:  StoredProcedure [dbo].[sp_createDemoShiftChangeReasonIfMissing]    Script Date: 10/29/2012 17:29:05 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create proc [dbo].[sp_createDemoShiftChangeReasonIfMissing](@changeReasonName varchar(50), @ChangeReasonType char(1), @propertyID int, @changeReasonID int OUTPUT) as
BEGIN
      select @changeReasonID = id from ShiftChangeReason where PropertyID = @propertyID and Name = @changeReasonName
      if @changeReasonID is null
      BEGIN
            insert into ShiftChangeReason (PropertyID, Name, ChangeReasonType) values (@propertyID, @changeReasonName, @ChangeReasonType)
            set @changeReasonID = SCOPE_IDENTITY()
      END
END

GO



/****** Object:  StoredProcedure [dbo].[sp_createDemoEarningCategoryIfMissing]    Script Date: 10/29/2012 17:29:19 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sp_createDemoEarningCategoryIfMissing]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[sp_createDemoEarningCategoryIfMissing]
GO



/****** Object:  StoredProcedure [dbo].[sp_createDemoEarningCategoryIfMissing]    Script Date: 10/29/2012 17:29:19 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create proc [dbo].[sp_createDemoEarningCategoryIfMissing](@earningCategoryName varchar(50), @UOM char(1), @propertyID int, @earningCategoryID int OUTPUT) as
BEGIN
      select @earningCategoryID = id from EarningCategory where PropertyID = @propertyID and Name = @earningCategoryName
      if @earningCategoryID is null
      BEGIN
            insert into EarningCategory (PropertyID, Name, UOM) values (@propertyID, @earningCategoryName, @UOM)
            set @earningCategoryID = SCOPE_IDENTITY()
      END
END

GO


/****** Object:  StoredProcedure [dbo].[sp_createDemoPortalMessage]    Script Date: 10/29/2012 17:28:48 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sp_createDemoPortalMessage]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[sp_createDemoPortalMessage]
GO

/****** Object:  StoredProcedure [dbo].[sp_createDemoPortalMessage]    Script Date: 10/29/2012 17:28:48 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create proc [dbo].[sp_createDemoPortalMessage](@fromEmployeeID int, @toEmployeeID int, @subject varchar(255), @body varchar(MAX), @shiftID int) as
BEGIN
      Declare @messageID int
      insert into PortalMessage (SentByEmployeeID, Subject, Message, Important, DateTimeSent, DateTimeExpires, MessageType, ShiftID) values (@fromEmployeeID, @subject, @body, 'N', getDate(), getDate() + 30, 'M', @shiftID)
      set @messageID = SCOPE_IDENTITY()
      insert into PortalMessageRecipient (PortalMessageID, EmployeeID, MessageRead, Pinned) values (@messageID, @toEmployeeID, 'N', 'N')
END

GO


