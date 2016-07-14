-- EmployeeShiftPunch

DROP INDEX ix_employeeshiftpunch1 ON dbo.EmployeeShiftPunch

ALTER TABLE dbo.EmployeeShiftPunch
      DROP CONSTRAINT pk_employeeshiftpunch


ALTER TABLE dbo.EmployeeShiftPunch ADD CONSTRAINT
      pk_employeeshiftpunch PRIMARY KEY NONCLUSTERED 
      (
      ID
      ) WITH( PAD_INDEX = OFF, FILLFACTOR = 80, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]


CREATE UNIQUE CLUSTERED INDEX ix_employeeshiftpunch1 ON dbo.EmployeeShiftPunch
      (
      EmployeeShiftID,
      RoundedTime,
      ID
      ) WITH( PAD_INDEX = OFF, FILLFACTOR = 80, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]


-- EmployeeShiftError

DROP INDEX ix_employeeshifterror1 ON dbo.EmployeeShiftError

ALTER TABLE dbo.EmployeeShiftError
      DROP CONSTRAINT pk_employeeshifterror

ALTER TABLE dbo.EmployeeShiftError ADD CONSTRAINT
      pk_employeeshifterror PRIMARY KEY NONCLUSTERED 
      (
      ID
      ) WITH( PAD_INDEX = OFF, FILLFACTOR = 80, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

CREATE UNIQUE CLUSTERED INDEX ix_employeeshifterror1 ON dbo.EmployeeShiftError
      (
      EmployeeShiftID,
      ID
      ) WITH( PAD_INDEX = OFF, FILLFACTOR = 80, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]


-- EmployeeShiftAdjustment

DROP INDEX ix_employeeshiftadjustment1 ON dbo.EmployeeShiftAdjustment

ALTER TABLE dbo.EmployeeShiftAdjustment
      DROP CONSTRAINT pk_employeeshiftadjustment

ALTER TABLE dbo.EmployeeShiftAdjustment ADD CONSTRAINT
      pk_employeeshiftadjustment PRIMARY KEY NONCLUSTERED 
      (
      ID
      ) WITH( PAD_INDEX = OFF, FILLFACTOR = 80, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

CREATE UNIQUE CLUSTERED INDEX ix_employeeshiftadjustment1 ON dbo.EmployeeShiftAdjustment
      (
      EmployeeShiftID,
      ID
      ) WITH( PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

-- EmployeeShift

DROP INDEX ix_employeeshift2 ON dbo.EmployeeShift

DROP INDEX ix_employeeshift1 ON dbo.EmployeeShift

ALTER TABLE dbo.EmployeeShift
      DROP CONSTRAINT pk_employeeshift

ALTER TABLE dbo.EmployeeShift ADD CONSTRAINT
      pk_employeeshift PRIMARY KEY NONCLUSTERED 
      (
      ID
      ) WITH( PAD_INDEX = OFF, FILLFACTOR = 80, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

CREATE UNIQUE CLUSTERED INDEX ix_employeeshift1 ON dbo.EmployeeShift
      (
      JobID,
      ShiftTypeCode,
      EmployeeID,
      ShiftDate,
      ID
      ) WITH( PAD_INDEX = OFF, FILLFACTOR = 80, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
