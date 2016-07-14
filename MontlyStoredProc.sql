USE [Josh_HiltonAPAC_9x]
GO

/****** Object:  StoredProcedure [dbo].[AddMonthlySchedulesNoPlanned] ******/
DROP PROCEDURE [dbo].[AddMonthlySchedulesNoPlanned]
GO

/****** Object:  StoredProcedure [dbo].[AddMonthlySchedulesNoPlanned] ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[AddMonthlySchedulesNoPlanned]
       @EmployeeID INT
AS 
       SET NOCOUNT ON;
--Find Existing shifts to delete if there are any.
IF EXISTS (SELECT 1 FROM EmployeeShift WHERE Employeeid = @EmployeeID AND shiftDate BETWEEN '07/01/12' AND '07/31/14')
       BEGIN
              DELETE FROM EmployeeShift WHERE Employeeid = @EmployeeID AND shiftDate BETWEEN '07/01/12' AND '07/31/14'
       END;

--get dates for Monday - Friday
WITH dates AS ( SELECT TOP ( 31 )
                        dates = DATEADD(DAY,
                                    ROW_NUMBER() OVER ( ORDER BY object_id ),
                                    '20140630')
               FROM     sys.all_objects
             ),
       noWeekend0 AS (            
    SELECT  *
    FROM    dates WHERE  DATENAME(dw,dates) NOT IN ('Saturday', 'Sunday')),
       noweekend AS
       (SELECT dates AS shiftDate, (dates + '12:00:000.00')AS startdate, (dates + '20:00:000.00') AS enddate FROM noWeekend0)

  INSERT INTO dbo.EmployeeShift
          ( ShiftDate ,
            ShiftTypeCode ,
            ShiftCategoryID ,
            Source ,
            CreatedOnDT ,
            AssignmentID ,
            WorkedHours ,
            AdjHours ,
            NetHours ,
            AvgRate ,
            NetDollars ,
            RegHours ,
            RegRate ,
            RegDollars ,
            AutoOTHours ,
            AdjOTHours ,
            OTHours ,
            OTRate ,
            OTDollars ,
            AutoDTHours ,
            AdjDTHours ,
            DTHours ,
            DTRate ,
            DTDollars ,
            StartTime ,
            EndTime ,
            EmpApprovedDT ,
            MgrApprovedDT ,
            MgrApprovedBy ,
            CheckID ,
            Notes ,
            Flags ,
            BankedHours ,
            PlannedShiftID ,
            Overlap ,
            EmployeeID ,
            JobID ,
            ReallocatedFromAdjustmentID ,
            PropertyID
          )
                     SELECT  
                     noweekend.shiftdate, -- ShiftDate - datetime
            'S' , -- ShiftTypeCode - char(1)
            5 , -- ShiftCategoryID - int
            'M' , -- Source - char(1)
            GETDATE() , -- CreatedOnDT - datetime
            NULL , -- AssignmentID - int
            8.00 , -- WorkedHours - decimal
            NULL , -- AdjHours - decimal
            8.00 , -- NetHours - decimal
            NULL , -- AvgRate - smallmoney
            NULL , -- NetDollars - decimal
            8.00 , -- RegHours - decimal
            NULL , -- RegRate - smallmoney
            NULL , -- RegDollars - decimal
            NULL , -- AutoOTHours - decimal
            NULL , -- AdjOTHours - decimal
            NULL , -- OTHours - decimal
            NULL , -- OTRate - smallmoney
            NULL , -- OTDollars - decimal
            NULL , -- AutoDTHours - decimal
            NULL , -- AdjDTHours - decimal
            NULL , -- DTHours - decimal
            NULL , -- DTRate - smallmoney
            NULL , -- DTDollars - decimal
            noWeekend.StartDate, -- StartTime - datetime
            noWeekend.EndDate, -- EndTime - datetime
            NULL , -- EmpApprovedDT - datetime
            NULL , -- MgrApprovedDT - datetime
            NULL , -- MgrApprovedBy - int
            NULL , -- CheckID - int
            NULL , -- Notes - nvarchar(200)
            NULL , -- Flags - int
            NULL , -- BankedHours - decimal
            NULL , -- PlannedShiftID - int
            'N' , -- Overlap - char(1)
            @EmployeeID , -- EmployeeID - int
            317 , -- JobID - int
            NULL , -- ReallocatedFromAdjustmentID - int
            3  -- PropertyID - int
                     FROM noWeekend
GO
