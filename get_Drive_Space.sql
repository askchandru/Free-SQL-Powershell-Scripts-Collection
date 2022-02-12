IF OBJECT_ID(N'tempdb..#Drives') IS NOT NULL
BEGIN
       DROP TABLE #Drives
END

IF OBJECT_ID(N'tempdb..#DrivesF') IS NOT NULL
BEGIN
       DROP TABLE #DrivesF
END

IF OBJECT_ID(N'tempdb..#Final') IS NOT NULL
BEGIN
       DROP TABLE #Final
END

DECLARE @Drive TINYINT, @SQL VARCHAR(100)
SET @Drive = 97
CREATE TABLE #Drives ( Drive VARCHAR(8), Info VARCHAR(128) )
WHILE @Drive <= 122
       BEGIN
             SET    @SQL = 'EXEC XP_CMDSHELL ''fsutil volume diskfree ' + CHAR(@Drive) + ':'''
             INSERT #Drives(Info) EXEC (@SQL)
             UPDATE #Drives SET Drive = CHAR(@Drive) WHERE  Drive IS NULL
             SET @Drive = @Drive + 1
       END

       --SELECT * FROM #Drives
       DELETE FROM #Drives WHERE info IS NULL
       DELETE FROM #Drives WHERE info = 'Error:  The system cannot find the path specified.'
       UPDATE #Drives SET Info = REPLACE(Info,' ','')
       UPDATE #Drives SET Info = REPLACE(Info,'Total#offreebytes:','Free - ')
       UPDATE #Drives SET Info = REPLACE(Info,'Total#ofbytes:','Total - ')
       DELETE FROM #Drives WHERE info LIKE 'Total#ofavailfreebytes%'
       SELECT * INTO #DrivesF FROM #Drives WHERE Info LIKE 'Free%'
       DELETE FROM #Drives WHERE Info LIKE 'Free%'
       SELECT @@SERVERNAME AS Server_Name,UPPER (A.Drive) AS Drive, CAST((REPLACE(A.Info,'Total - ','')) AS BIGINT)/1024/1024/1024 AS Total_Space_GB ,CAST((REPLACE(B.Info,'Free - ','')) AS BIGINT)/1024/1024/1024 AS Free_Space_GB  FROM #Drives a JOIN #DrivesF b ON a.Drive = b.Drive
       
