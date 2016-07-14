USE uniportal
GO

select DatabaseURL from watsonDatabase where databaseUrl LIKE '%ctx%' AND DatabaseURL NOT IN 
				(
					select '//ctx-du-sql-002/' + Name from sys.databases)
					
/*DELETE from watsonDatabase where databaseUrl LIKE '%ctx%' AND DatabaseURL NOT IN 
				(
					select '//ctx-du-sql-002/' + Name from sys.databases)*/
select * from watsonDatabase where databaseUrl LIKE '%ctx%' AND DatabaseURL NOT IN 
				(
					select '//ctx-du-sql-002/' + Name from sys.databases)

