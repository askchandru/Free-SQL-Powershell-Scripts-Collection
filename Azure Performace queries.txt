--Query the top recent 15 queries by CPU usage

WITH AggregatedCPU AS 
    (SELECT
        q.query_hash, 
        SUM(count_executions * avg_cpu_time / 1000.0) AS total_cpu_ms, 
        SUM(count_executions * avg_cpu_time / 1000.0)/ SUM(count_executions) AS avg_cpu_ms, 
        MAX(rs.max_cpu_time / 1000.00) AS max_cpu_ms, 
        MAX(max_logical_io_reads) max_logical_reads, 
        COUNT(DISTINCT p.plan_id) AS number_of_distinct_plans, 
        COUNT(DISTINCT p.query_id) AS number_of_distinct_query_ids, 
        SUM(CASE WHEN rs.execution_type_desc='Aborted' THEN count_executions ELSE 0 END) AS aborted_execution_count, 
        SUM(CASE WHEN rs.execution_type_desc='Regular' THEN count_executions ELSE 0 END) AS regular_execution_count, 
        SUM(CASE WHEN rs.execution_type_desc='Exception' THEN count_executions ELSE 0 END) AS exception_execution_count, 
        SUM(count_executions) AS total_executions, 
        MIN(qt.query_sql_text) AS sampled_query_text
    FROM sys.query_store_query_text AS qt
    JOIN sys.query_store_query AS q ON qt.query_text_id=q.query_text_id
    JOIN sys.query_store_plan AS p ON q.query_id=p.query_id
    JOIN sys.query_store_runtime_stats AS rs ON rs.plan_id=p.plan_id
    JOIN sys.query_store_runtime_stats_interval AS rsi ON rsi.runtime_stats_interval_id=rs.runtime_stats_interval_id
    WHERE 
            rs.execution_type_desc IN ('Regular', 'Aborted', 'Exception') AND 
        rsi.start_time>=DATEADD(HOUR, -2, GETUTCDATE())
     GROUP BY q.query_hash), 
OrderedCPU AS 
    (SELECT *, 
    ROW_NUMBER() OVER (ORDER BY total_cpu_ms DESC, query_hash ASC) AS RN
    FROM AggregatedCPU)
SELECT *
FROM OrderedCPU AS OD
WHERE OD.RN<=15
ORDER BY total_cpu_ms DESC;
GO

--Query the most frequently compiled queries by query hash


SELECT TOP (20)
    query_hash,
    MIN(initial_compile_start_time) as initial_compile_start_time,
    MAX(last_compile_start_time) as last_compile_start_time,
    CASE WHEN DATEDIFF(mi,MIN(initial_compile_start_time), MAX(last_compile_start_time)) > 0
        THEN 1.* SUM(count_compiles) / DATEDIFF(mi,MIN(initial_compile_start_time), 
            MAX(last_compile_start_time)) 
        ELSE 0 
        END as avg_compiles_minute,
    SUM(count_compiles) as count_compiles
FROM sys.query_store_query AS q
GROUP BY query_hash
ORDER BY count_compiles DESC;
GO

--Identify the CPU usage and query plan for a given query hash


declare @query_hash binary(8);

SET @query_hash = 0x6557BE7936AA2E91;

with query_ids as (
    SELECT
        q.query_hash,
        q.query_id,
        p.query_plan_hash,
        SUM(qrs.count_executions) * AVG(qrs.avg_cpu_time)/1000. as total_cpu_time_ms,
        SUM(qrs.count_executions) AS sum_executions,
        AVG(qrs.avg_cpu_time)/1000. AS avg_cpu_time_ms
    FROM sys.query_store_query q
    JOIN sys.query_store_plan p on q.query_id=p.query_id
    JOIN sys.query_store_runtime_stats qrs on p.plan_id = qrs.plan_id
    WHERE q.query_hash = @query_hash
    GROUP BY q.query_id, q.query_hash, p.query_plan_hash)
SELECT qid.*,
    qt.query_sql_text,
    p.count_compiles,
    TRY_CAST(p.query_plan as XML) as query_plan
FROM query_ids as qid
JOIN sys.query_store_query AS q ON qid.query_id=q.query_id
JOIN sys.query_store_query_text AS qt on q.query_text_id = qt.query_text_id
JOIN sys.query_store_plan AS p ON qid.query_id=p.query_id and qid.query_plan_hash=p.query_plan_hash
ORDER BY total_cpu_time_ms DESC;
GO



--INDEX Recommendation 

select * from sys.dm_db_tuning_recommendations


WITH DbTuneRec
AS (SELECT ddtr.reason,
ddtr.score,
pfd.query_id,
pfd.regressedPlanId,
pfd.recommendedPlanId,
JSON_VALUE(ddtr.state,
'$.currentValue') AS CurrentState,
JSON_VALUE(ddtr.state,
'$.reason') AS CurrentStateReason,
JSON_VALUE(ddtr.details,
'$.implementationDetails.script') AS ImplementationScript
FROM sys.dm_db_tuning_recommendations AS ddtr
CROSS APPLY
OPENJSON(ddtr.details,
'$.planForceDetails')
WITH (query_id INT '$.queryId',
regressedPlanId INT '$.regressedPlanId',
recommendedPlanId INT '$.recommendedPlanId') AS pfd)
SELECT qsq.query_id,
dtr.reason,
dtr.score,
dtr.CurrentState,
dtr.CurrentStateReason,
qsqt.query_sql_text,
CAST(rp.query_plan AS XML) AS RegressedPlan,
CAST(sp.query_plan AS XML) AS SuggestedPlan,
dtr.ImplementationScript
FROM DbTuneRec AS dtr
JOIN sys.query_store_plan AS rp
ON rp.query_id = dtr.query_id
AND rp.plan_id = dtr.regressedPlanId
JOIN sys.query_store_plan AS sp
ON sp.query_id = dtr.query_id
AND sp.plan_id = dtr.recommendedPlanId
JOIN sys.query_store_query AS qsq
ON qsq.query_id = rp.query_id
JOIN sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id;


--Index recommend by Azure portal we can extract from tsql as well

CREATE TABLE #test11 ([key] VARCHAR(128),[value] NVARCHAR(MAX), [type] INT )
DECLARE @data NVARCHAR(MAX);

SELECT name,details INTO #test12 FROM sys.dm_db_tuning_recommendations  

DECLARE @Counter INT 
DECLARE @na VARCHAR(128) 

SELECT  @Counter= COUNT(*) FROM #test12
WHILE ( @Counter > 0)
BEGIN
    SET @na = (SELECT TOP 1 name FROM #test12)
    SET @data = (SELECT details FROM #test12 WHERE name = @na )
	INSERT INTO #test11 SELECT *  FROM OpenJson(@data);
	DELETE FROM #test12 WHERE name = @na 
	SELECT  @Counter= count(*) FROM #test12
END

DELETE FROM #test11 WHERE [key] NOT IN('implementationDetails')
DELETE FROM #test11 WHERE [value] LIKE '%sp_query_store_force_plan%'

SELECT DB_Name() AS DatabaseName, REPLACE(REPLACE(REPLACE(REPLACE([value],'{',''),'"method":"TSql",',''),'"script":"',''),'"','') AS Recomended_Index from #test11

DROP TABLE #test11
DROP TABLE #test12