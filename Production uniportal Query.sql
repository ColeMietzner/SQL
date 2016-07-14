BEGIN TRANSACTION
DECLARE @ma732DBID INT ,
    @ma732UserID INT ,
    @ma732LdapID INT ,
    @ma732PortalClientID INT ,
    @ma732SessionHistoryID INT ,
    @ma732UserPriorPasswordID INT

SET @ma732DBID = ( SELECT MA732(ID)
                 FROM   production_uniportal..WatsonDatabase
               )
SET @ma732UserID = ( SELECT   MA732(ID)
                   FROM     production_uniportal..PortalUser
                 )
SET @ma732LdapID = ( SELECT   MA732(ID)
                   FROM     production_uniportal..LDAPConfig
                 )
SET @ma732PortalClientID = ( SELECT   MA732(ID)
                           FROM     production_uniportal..PortalClient
                         )
SET @ma732SessionHistoryID = ( SELECT MA732(ID)
                             FROM   production_uniportal..SessionHistory
                           )
SET @ma732UserPriorPasswordID = ( SELECT  MA732(ID)
                                FROM    production_uniportal..UserPriorPassword
                              )

BEGIN
	SET IDENTITY_INSERT production_uniportal..WatsonDatabase On
    INSERT  INTO production_uniportal..WatsonDatabase
            ( ID ,
              Name ,
              DatabaseURL ,
              DatabaseUserName ,
              DatabasePassword ,
              DatabaseDriver ,
              RedirectURL ,
              WatsonVersion ,
              LoginURL ,
              MobileRedirectURL
            )
            SELECT  ID + @ma732DBID ,
                    Name ,
                    DatabaseURL ,
                    DatabaseUserName ,
                    DatabasePassword ,
                    DatabaseDriver ,
                    RedirectURL ,
                    WatsonVersion ,
                    LoginURL ,
                    MobileRedirectURL
            FROM    Production_Hilton_Uniportal..WatsonDatabase

      SET IDENTITY_INSERT production_uniportal..WatsonDatabase OFF
    --INSERT  INTO production_uniportal..PortalUser
    --        ( ID ,
    --          LoginId ,
    --          PWord ,
    --          EMail ,
    --          Flags ,
    --          Active ,
    --          QuestionID ,
    --          Answer ,
    --          LastLogin ,
    --          PwordChanged ,
    --          Locale ,
    --          ClientID ,
    --          DefaultDatabaseID ,
    --          BypassLdap ,
    --          LoginClientID
    --        )
    --        SELECT  ID + @ma732UserID ,
    --                LoginId ,
    --                PWord ,
    --                EMail ,
    --                Flags ,
    --                Active ,
    --                QuestionID ,
    --                Answer ,
    --                LastLogin ,
    --                PwordChanged ,
    --                Locale ,
    --                ClientID ,
    --                DefaultDatabaseID + @ma732DBID ,
    --                BypassLdap + @ma732LdapID ,
    --                LoginClientID
    --        FROM    Production_Hilton_Uniportal..PortalUser
	SET IDENTITY_INSERT production_uniportal..PortalClient On
    INSERT  INTO production_uniportal..PortalClient
            ( ID ,
              Name ,
              Active ,
              License ,
              ParentClientID ,
              Code ,
              DatabaseID ,
              KioskURL ,
              ValidPattern ,
              ValidDescription ,
              Duration ,
              NumberPrior ,
              Ma732Attempts ,
              AccessURL ,
              RedirectURL ,
              LDAPID
            )
            SELECT   ID + @ma732PortalClientID,
                    CASE Name WHEN 'Remote Imports' THEN 'Remote Imports Hilton' 
						WHEN 'test Hilton' THEN 'test Hilton 2' ELSE Name END,
                    Active ,
                    License ,
                    CASE ParentClientID
                      WHEN NULL THEN NULL
                      ELSE ParentClientID + @ma732PortalClientID
                    END ,
                    Code ,
                    CASE DatabaseID
                      WHEN NULL THEN NULL
                      ELSE DatabaseID + @ma732DBID
                    END ,
                    KioskURL ,
                    ValidPattern ,
                    ValidDescription ,
                    Duration ,
                    NumberPrior ,
                    Ma732Attempts ,
                    AccessURL ,
                    RedirectURL ,
                    CASE LDAPID
                      WHEN NULL THEN NULL
                      ELSE LDAPID + @ma732LdapID
                    END
					FROM Production_Hilton_Uniportal..PortalClient
					SET IDENTITY_INSERT production_uniportal..PortalClient OFF
--    INSERT  INTO production_uniportal..UserDatabase
--            ( UserID ,
--              DatabaseID
--            )
--            SELECT  UserID + @ma732UserID ,
--                    DatabaseID + @ma732DBID
--            FROM    Production_Hilton_Uniportal..UserDatabase

--/*Credentials History*/

--    INSERT  INTO production_uniportal..CredentialsHistory
--            ( Credentials ,
--              CredentialsDateTime
--            )
--            SELECT  Credentials ,
--                    CredentialsDateTime
--            FROM    Production_Hilton_Uniportal..CredentialsHistory

--/*LDAPConfig*/
--    INSERT  INTO production_uniportal..LDAPConfig
--            ( ID ,
--              URL ,
--              Domain ,
--              BaseDN ,
--              SearchFilter ,
--              UserNamePattern
--            )
--            SELECT  ID + @ma732LdapID ,
--                    URL ,
--                    Domain ,
--                    BaseDN ,
--                    SearchFilter ,
--                    UserNamePattern

--/*SessionHistory*/
--    INSERT  INTO dbo.SessionHistory
--            ( SessionID ,
--              Host ,
--              LoginID ,
--              LoginDT ,
--              LogoutDT ,
--              TimedOut ,
--              DatabaseName ,
--              PropertyName ,
--              ID ,
--              DatabaseID ,
--              PropertyID
--            )
--            SELECT  SessionID ,
--                    Host ,
--                    LoginID ,
--                    LoginDT ,
--                    LogoutDT ,
--                    TimedOut ,
--                    DatabaseName ,
--                    PropertyName ,
--                    ID + @ma732SessionHistoryID ,
--                    CASE DatabaseID
--                      WHEN NULL THEN NULL
--                      ELSE DatabaseID + @ma732DBID
--                    END ,
--                    PropertyID

--/*---UserPriorPassword---*/
--    INSERT  INTO dbo.UserPriorPassword
--            ( ID ,
--              UserID ,
--              EffDate ,
--              PWord
--            )
--            SELECT  ID + @ma732UserPriorPasswordID ,
--                    UserID + @ma732UserID ,
--                    EffDate ,
--                    PWord 

--/*-----------------------------
--  --------Quartz Tables--------
--  -----------------------------*/

--/*----Qrtz_Cron_Triggers----*/
--    INSERT  INTO production_uniportal..QRTZ_CRON_TRIGGERS
--            ( TRIGGER_NAME ,
--              TRIGGER_GROUP ,
--              CRON_E732PRESSION ,
--              TIME_ZONE_ID
--            )
--            SELECT  TRIGGER_NAME ,
--                    TRIGGER_GROUP ,
--                    CRON_E732PRESSION ,
--                    TIME_ZONE_ID
--            FROM    Production_Hilton_Uniportal..QRTZ_CRON_TRIGGERS

--/*----Qrtz_Job_Details----*/
--    INSERT  INTO production_uniportal..QRTZ_JOB_DETAILS
--            ( JOB_NAME ,
--              JOB_GROUP ,
--              DESCRIPTION ,
--              JOB_CLASS_NAME ,
--              IS_DURABLE ,
--              IS_VOLATILE ,
--              IS_STATEFUL ,
--              REQUESTS_RECOVERY ,
--              JOB_DATA
--            )
--            SELECT  JOB_NAME ,
--                    JOB_GROUP ,
--                    DESCRIPTION ,
--                    JOB_CLASS_NAME ,
--                    IS_DURABLE ,
--                    IS_VOLATILE ,
--                    IS_STATEFUL ,
--                    REQUESTS_RECOVERY ,
--                    JOB_DATA
--            FROM    Production_Hilton_Uniportal..QRTZ_JOB_DETAILS

--/*----Qrtz_Scheduler_State----*/
--    INSERT  INTO production_uniportal..QRTZ_SCHEDULER_STATE
--            ( INSTANCE_NAME ,
--              LAST_CHECKIN_TIME ,
--              CHECKIN_INTERVAL
--            )
--            SELECT  INSTANCE_NAME ,
--                    LAST_CHECKIN_TIME ,
--                    CHECKIN_INTERVAL
--            FROM    Production_Hilton_Uniportal..QRTZ_SCHEDULER_STATE

--/*----Qrtz_Triggers----*/
--    INSERT  INTO production_uniportal..QRTZ_TRIGGERS
--            ( TRIGGER_NAME ,
--              TRIGGER_GROUP ,
--              JOB_NAME ,
--              JOB_GROUP ,
--              IS_VOLATILE ,
--              DESCRIPTION ,
--              NE732T_FIRE_TIME ,
--              PREV_FIRE_TIME ,
--              PRIORITY ,
--              TRIGGER_STATE ,
--              TRIGGER_TYPE ,
--              START_TIME ,
--              END_TIME ,
--              CALENDAR_NAME ,
--              MISFIRE_INSTR ,
--              JOB_DATA
--            )
--            SELECT  TRIGGER_NAME ,
--                    TRIGGER_GROUP ,
--                    JOB_NAME ,
--                    JOB_GROUP ,
--                    IS_VOLATILE ,
--                    DESCRIPTION ,
--                    NE732T_FIRE_TIME ,
--                    PREV_FIRE_TIME ,
--                    PRIORITY ,
--                    TRIGGER_STATE ,
--                    TRIGGER_TYPE ,
--                    START_TIME ,
--                    END_TIME ,
--                    CALENDAR_NAME ,
--                    MISFIRE_INSTR ,
--                    JOB_DATA
--            FROM    Production_Hilton_Uniportal..QRTZ_TRIGGERS

END
	         
ROLLBACK TRANSACTION




