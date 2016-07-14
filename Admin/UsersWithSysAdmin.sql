/*	Security is a very important aspect that a DBA should know about. It is also important to know which login has a sysadmin or security admin 
	server level role. The following SQL Command will show information related to the security admin server role and system admin server role.*/

SELECT  l.name ,
        l.denylogin ,
        l.isntname ,
        l.isntgroup ,
        l.isntuser
FROM    master.dbo.syslogins l
WHERE   l.sysadmin = 1
        OR l.securityadmin = 1