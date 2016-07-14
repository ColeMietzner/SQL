select * from WatsonDatabase
select * from portaluser
select * from userdatabase
select IDENT_SEED('LoginHistory')
update watsondatabase set databaseurl ='//DCO-LT-002/hiltonus' where id = 1

select * from WatsonGroup where id in (1, 23, 24)

delete from WatsonGroupActionSecurity where SecurityCode = 'REVENUE_DASHBOARD'

insert into WatsonGroupActionSecurity (GroupID,SecurityCode,AccessCode) select GroupID,'REVENUE_DASHBOARD','F' from
         WatsonGroupActionSecurity where SecurityCode = 'AD_HOC_REPORTS'


		 USE [hiltonus]
GO

/****** Object:  Trigger [dbo].[watsongroupactionsecurity_update]    Script Date: 2/21/2013 2:45:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

alter trigger [dbo].[watsongroupactionsecurity_update] on [dbo].[WatsonGroupActionSecurity] for insert,update as
   if update(GroupID)
   begin
      if (select count(*) from Inserted where GroupID is not null) > 0
      begin
         if (select count(*) from WatsonGroup p join Inserted i on p.ID = i.GroupID) = 0
         begin
            rollback transaction
            raiserror ('The GroupID''s value doesn''t exist in the WatsonGroup table.',16,1)
            return
         end
      end
   end
GO

alter database uniportal set compatibility_level = 90

