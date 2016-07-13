Use Mart
Go

DECLARE @ttlProviders NVARCHAR(10) = ( SELECT COUNT(1) FROM ODS1.Show.SOLRProvider);
DECLARE @loopcount INT = 0;
DECLARE @rowCount INT = ( SELECT COUNT(1) FROM DataBackup.QA.BIMartAccuracyDriver);
DECLARE @sqlCommand NVARCHAR(MAX) = '';
DECLARE @ElementType VARCHAR(50);
DECLARE @destinationColumn VARCHAR(50);
DECLARE @sourceColumn VARCHAR(50);
DECLARE @destination VARCHAR(50);
DECLARE @source VARCHAR(50);
DECLARE @sourceJoinKey VARCHAR(25);
DECLARE @destinationJoinKey VARCHAR(25);
DECLARE @joinCriteria VARCHAR(255);
DECLARE @xmlElementPath VARCHAR(255);
DECLARE @SourceJoinAlias VARCHAR(25);
DECLARE @SourceJoin VARCHAR(255);
DECLARE @DestinationJoinAlias VARCHAR(255);
DECLARE @CustomQuery VARCHAR(255);
DECLARE @SourceFilter VARCHAR(255);

WHILE @loopcount < @rowCount
    BEGIN	
	BEGIN TRY
	BEGIN TRANSACTION
	    SET @ElementType = (SELECT [ElementType] FROM DataBackup.QA.BIMartAccuracyDriver WHERE DriverID = @loopcount);
        SET @sourceColumn = ( SELECT [SourceColumn] FROM DataBackup.QA.BIMartAccuracyDriver WHERE DriverID = @loopcount);
        SET @destinationColumn = ( SELECT [MartColumn] FROM DataBackup.QA.BIMartAccuracyDriver WHERE DriverID = @loopcount);
        SET @destination = ( SELECT [Destination] FROM DataBackup.QA.BIMartAccuracyDriver WHERE DriverID = @loopcount);
        SET @source = ( SELECT  [Source] FROM DataBackup.QA.BIMartAccuracyDriver WHERE DriverID = @loopcount);
        SET @sourceJoinKey = ( SELECT SourceJoinKey FROM DataBackup.QA.BIMartAccuracyDriver WHERE DriverID = @loopcount); 
        SET @destinationJoinKey = ( SELECT DestinationJoinKey FROM DataBackup.QA.BIMartAccuracyDriver WHERE DriverID = @loopcount); 
        SET @joinCriteria = ( SELECT JoinCriteria FROM DataBackup.QA.BIMartAccuracyDriver WHERE DriverID = @loopcount);
		SET @xmlElementPath = ( SELECT XMLElementPath FROM DataBackup.QA.BIMartAccuracyDriver WHERE DriverID = @loopcount);
		SET @SourceJoinAlias = (SELECT SourceJoinAlias FROM DataBackup.QA.BIMartAccuracyDriver WHERE DriverID = @loopcount);
		SET @DestinationJoinAlias = (SELECT destinationJoinAlias FROM DataBackup.QA.BIMartAccuracyDriver WHERE DriverID = @loopcount);
		SET @sourceJoin = (SELECT SourceJoin FROM DataBackup.QA.BIMartAccuracyDriver WHERE DriverID = @loopcount);
		SET @CustomQuery = (SELECT CustomQuery FROM DataBackup.QA.BIMartAccuracyDriver WHERE DriverID = @loopcount);
		SET @SourceFilter = (SELECT SourceFIlter FROM DataBackup.QA.BIMartAccuracyDriver WHERE DriverID = @loopcount);
        IF @ElementType = 1 
		BEGIN
	    SET @sqlCommand = N'WITH mart
          AS ( SELECT ' + @destinationColumn + ', ' + @destinationJoinKey  
		  IF  @SourceJoinAlias IS NOT NULL 
		  BEGIN
		  SELECT @sqlCommand += N', ' + @SourceJoinAlias 
		  END
          SET @sqlCommand +=
               ' FROM ' + @destination 
			   IF @SourceJoin IS NOT NULL
			   BEGIN
					select @sqlCommand += ' ' + @SourceJoin 
					END
				SET @sqlCommand += N'),
        solr
          AS ( SELECT  ' + @sourceColumn + ', ' + @sourceJoinKey + '
               FROM ' + @source + '
             ),  
			 
        counts
          AS ( SELECT  mart.' + @destinationColumn + ' as mart , 
		  solr.' + @sourceColumn + ' as solr,
                        CASE WHEN ( mart.' + @destinationColumn + ' <> solr.'
            + @sourceColumn + ' ) THEN 1
                             ELSE 0
                        END AS cnt
               FROM     mart ' + @joinCriteria + '
             WHERE  mart.' + @destinationColumn + ' <> solr.' + @sourceColumn
            + '
             ) 
     SELECT  getdate() as DateInserted, ''' + @sourceColumn +''' as SourceColumn,''' + @DestinationColumn + ''' as DestinationColumn,  SUM(cnt) AS CountWrong, 
            CAST(SUM(counts.Cnt) AS decimal(9,2)) / ' + @ttlProviders
            + ' * 100 AS [PercentWrong]
    FROM    counts;';
		
		END

		ELSE IF @ElementType = 2
		BEGIN
        	    SET @sqlCommand = N'WITH mart
          AS ( SELECT ' + @destinationColumn + N', ' + @destinationJoinKey  
		  IF  @SourceJoinAlias IS NOT NULL 
		  BEGIN
		  SELECT @sqlCommand += N', ' + @SourceJoinAlias +' AS ' + @destinationJoinAlias
		  END
          SET @sqlCommand +=
               ' FROM ' + @destination + ' '
			   IF @SourceJoin IS NOT NULL
			   BEGIN
					select @sqlCommand += @SourceJoin 
					END
				SET @sqlCommand += N'),
        solr
          AS ( SELECT  ' + @sourceColumn + '.value(' + @xmlElementPath + ')' 
		  IF @destinationJoinAlias IS NOT NULL
          BEGIN
          SET @sqlCommand += ' AS ' + @destinationJoinAlias
		  END
          SET @sqlCommand += 
		  ' , ' + @sourceJoinKey + '
               FROM ' + @source + '
             ),
        counts
          AS ( SELECT  mart.' + @destinationColumn + ', 
		  solr.' + @destinationJoinAlias + ' ,
                                                               CASE WHEN ( mart.' + @destinationJoinAlias + ' <> solr.'
            + @DestinationJoinAlias + ' ) THEN 1
                             ELSE 0
                        END AS cnt
               FROM     mart ' + @joinCriteria + '
             WHERE  mart.' + @destinationJoinAlias + ' <> solr.' + @destinationJoinAlias
            + '
             )  
     SELECT getdate() as DateInserted, ''' + @sourceColumn +''' as SourceColumn,''' + @DestinationColumn + ''' as DestinationColumn,  SUM(cnt) AS CountWrong, 
            CAST(SUM(counts.Cnt) AS decimal(9,2)) / ' + @ttlProviders
            + ' * 100 AS [PercentWrong]

    FROM    counts; ';
END
		ELSE IF @ElementType = 4
		BEGIN
        	    SET @sqlCommand = N'WITH mart
          AS ( SELECT ' + @destinationColumn + N', ' + @destinationJoinKey  
		  IF  @SourceJoinAlias IS NOT NULL 
		  BEGIN
		  SELECT @sqlCommand = @sqlCommand --+ N', ' + @SourceJoinAlias +' AS ' + @destinationJoinAlias
		  END
          SET @sqlCommand +=
               ' FROM ' + @destination + ' '
			   IF @SourceJoin IS NOT NULL
			   BEGIN
					select @sqlCommand += ' ' + @SourceJoin 
					END
				SET @sqlCommand = @sqlCommand 
				IF @sourceFilter IS NOT NULL
				BEGIN
				SELECT @sqlCommand += @sourceFilter
				END
				SET @sqlCommand += N'),
        solr
          AS ( SELECT  ' 
		 
		  IF @CustomQuery IS NOT NULL
		  BEGIN 
		  SELECT @sqlCommand += @customQuery 
		  END
		  SET @sqlCommand = @sqlCommand
		  SET @sqlCommand += 
		  ' , ' + @sourceJoinKey + '
               FROM ' + @source + '
             ),
        counts
          AS ( SELECT  mart.' + @destinationColumn + ' as mart , 
		  solr.' + @SourceJoinAlias + ' as solr,
                        CASE WHEN ( mart.' + @destinationColumn + ' <> solr.'
            + @SourceJoinAlias + ' ) THEN 1
                             ELSE 0
                        END AS cnt
               FROM     mart ' + @joinCriteria + '
             WHERE  mart.' + @destinationColumn + ' <> solr.' + @SourceJoinAlias
            + '
             ) 
    SELECT  getdate() as DateInserted, ''' + @sourceColumn +''' as SourceColumn,''' + @DestinationColumn + ''' as DestinationColumn,  SUM(cnt) AS CountWrong, 
            CAST(SUM(counts.Cnt) AS decimal(9,2)) / ' + @ttlProviders
            + ' * 100 AS [PercentWrong]

    FROM    counts; ';
	END
	
        SET @loopcount += 1;
	--PRINT @sqlCommand
		INSERT INTO Audit.entityAccuracy EXECUTE sp_executeSQL @sqlCommand
		commit
        END TRY	
		BEGIN CATCH
			ROLLBACK;
			THROW;
			BREAK;
		END CATCH
    END


