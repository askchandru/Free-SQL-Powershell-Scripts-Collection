use tempdb
go
IF OBJECT_ID(N'dbo.log', N'U') IS NOT NULL  
   DROP TABLE [dbo].[log];  
GO
IF OBJECT_ID(N'dbo.data', N'U') IS NOT NULL  
   DROP TABLE [dbo].[data];  
GO
IF OBJECT_ID(N'dbo.final', N'U') IS NOT NULL  
   DROP TABLE [dbo].[final];  
GO

declare @dqry VARCHAR(MAX), @db varchar(max)
CREATE TABLE [dbo].[data]( [DbName] [nvarchar](128) NULL, File_Size_MB [numeric](20, 6) NULL, Space_Used_MB [numeric](20, 6) NULL,Free_Space_MB [numeric](20, 6) NULL)

CREATE TABLE [dbo].[log]( [DbName] [nvarchar](128) NULL, File_Size_MB [numeric](20, 6) NULL, Space_Used_MB [numeric](20, 6) NULL,Free_Space_MB [numeric](20, 6) NULL)
CREATE TABLE [dbo].[final]( [DbName] [nvarchar](128) NULL, [Data_File_Size_MB] [numeric](20, 6) NULL, [Data_Space_Used_MB] [numeric](20, 6) NULL,
       [Data_Free_Space_MB] [numeric](20, 6) NULL, [Log_File_Size_MB] [numeric](20, 6) NULL, [Log_Space_Used_MB] [numeric](20, 6) NULL, [Log_Free_Space_MB] [numeric](20, 6) NULL)


DECLARE CurRestore CURSOR FOR SELECT name FROM sysdatabases where name not in('KPIRepository','Policy_Management_Archive')
OPEN CurRestore

FETCH NEXT FROM CurRestore INTO  @db

WHILE @@FETCH_STATUS=0
BEGIN
SET @dqry = ''
SET @dqry = @dqry + 'use ' + @db + ';' + char(10)+ 'truncate table tempdb..[data]' + char(10) + 'truncate table tempdb..[log]' + char(10)

SET @dqry = @dqry + 'insert into tempdb..[data] select DB_NAME() AS DbName, sum(size)/128.0 AS File_Size_MB,' + char(10)
SET @dqry = @dqry + 'sum(CAST(FILEPROPERTY(name, ''SpaceUsed'') AS INT))/128.0 as Space_Used_MB, SUM( size)/128.0 - sum(CAST(FILEPROPERTY(name,''SpaceUsed'') AS INT))/128.0 AS Free_Space_MB   from ' + char(10)
SET @dqry = @dqry + 'sys.database_files where type in(0) group by type' + char(10)

SET @dqry = @dqry + 'insert into tempdb..[log] select DB_NAME() AS DbName, sum(size)/128.0 AS File_Size_MB,' + char(10)
SET @dqry = @dqry + 'sum(CAST(FILEPROPERTY(name, ''SpaceUsed'') AS INT))/128.0 as Space_Used_MB, SUM( size)/128.0 - sum(CAST(FILEPROPERTY(name,''SpaceUsed'') AS INT))/128.0 AS Free_Space_MB  from ' + char(10)
SET @dqry = @dqry + 'sys.database_files where type in(1) group by type' + char(10)

SET @dqry = @dqry + 'insert into tempdb..[final] select a.DbName, a.File_Size_MB as Data_File_Size_MB,a.Space_Used_MB as Data_Space_Used_MB,a.Free_Space_MB as Data_Free_Space_MB,' + char(10)
SET @dqry = @dqry + 'b.File_Size_MB as Log_File_Size_MB,b.Space_Used_MB as Log_Space_Used_MB,b.Free_Space_MB as Log_Free_Space_MB from tempdb..[data] a join tempdb..[log] b on 1=1' + char(10)

EXEC(@dqry)

FETCH NEXT FROM CurRestore INTO  @db


END

CLOSE CurRestore
DEALLOCATE CurRestore


select * from final

IF OBJECT_ID(N'dbo.log', N'U') IS NOT NULL  
   DROP TABLE [dbo].[log];  
GO
IF OBJECT_ID(N'dbo.data', N'U') IS NOT NULL  
   DROP TABLE [dbo].[data];  
GO
IF OBJECT_ID(N'dbo.final', N'U') IS NOT NULL  
   DROP TABLE [dbo].[final];  
GO
