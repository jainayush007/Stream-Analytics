CREATE OR REPLACE PROCEDURE `project-id.curated.sp_debezium_log_merge` (dbtbname STRING, colname STRING,pk STRING,region STRING)
BEGIN

DECLARE col_diff INT64;
DECLARE cols STRING;
DECLARE join_key STRING;
SET join_key= (select replace(pk,',','||'));
select join_key;

EXECUTE IMMEDIATE FORMAT("""
  CREATE OR REPLACE VIEW  """||dbtbname||"""_log_v AS(
    SELECT
      message_id,
      JSON_VALUE(data.payload.op) op,
      """ || colname ||""",
      TIMESTAMP_MILLIS(CAST(JSON_VALUE(data.payload.source.ts_ms) AS INT64)) source_ts,
      publish_time,
      bq_load_ts,
      JSON_VALUE(data.payload.source.db) ||'.'|| JSON_VALUE(data.payload.source.table) source_db_table,
      subscription_name,
      CAST(JSON_VALUE(data.payload.source.pos) AS INT64) pos
    FROM
      """ ||dbtbname||"""_log
    WHERE
      JSON_VALUE(data.payload.op)IS NOT NULL
    ORDER BY
      source_ts DESC,pos DESC); """);


--DELETE FROM test.column_diff WHERE table_name= (select format('%s',dbtbname));
  execute immediate format("""
  select DISTINCT 1 from(
--          INSERT INTO test.column_diff
            SELECT '"""|| dbtbname ||"""_log_v' table,* FROM
            (SELECT column_name,data_type FROM """ ||region||""".INFORMATION_SCHEMA.COLUMNS where trim(table_catalog)||'.'||trim(table_schema)||'.'||trim(table_name)='"""|| dbtbname ||"""_log_v' and column_name <> 'op'
            EXCEPT DISTINCT
            SELECT column_name,data_type FROM """ ||region||""".INFORMATION_SCHEMA.COLUMNS where trim(table_catalog)||'.'||trim(table_schema)||'.'||trim(table_name)='"""|| dbtbname ||"""')
  )
    """) into col_diff; 
--SELECT col_diff;

IF 1 = (SELECT col_diff )
--IF EXISTS (SELECT 1 FROM test.column_diff WHERE table_name= (select format('%s',dbtbname)) ) 
THEN
--DECLARE dbtbname STRING DEFAULT "project-id.test.debezium_usertable";
--DECLARE pk STRING DEFAULT "ycsb_key";
  execute immediate format("""
    CREATE OR REPLACE TABLE """|| dbtbname ||"""
    PARTITION BY DATETIME_TRUNC(publish_time,DAY)
    CLUSTER BY """|| pk ||""" AS(
    SELECT * EXCEPT(row_num,op) 
    from
    (SELECT * , row_number() over(partition by """|| pk ||""" order  by source_ts DESC,pos DESC) row_num
    FROM """|| dbtbname ||"""_log_v
      ) log_latest
    where row_num=1
    and op <> 'd');
  """);

ELSE
    
    BEGIN
      BEGIN TRANSACTION;

--DECLARE dbtbname STRING DEFAULT "project-id.test.debezium_usertable";
--DECLARE pk STRING DEFAULT "ycsb_key";
  execute immediate format("""
      DELETE FROM """|| dbtbname ||"""
      WHERE """|| join_key ||""" IN (SELECT DISTINCT """|| join_key ||""" FROM """|| dbtbname ||"""_log_v where publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 2 DAY));
  """);
  
  
  execute immediate format("""  
      SELECT string_agg(column_name) FROM """ ||region||""".INFORMATION_SCHEMA.COLUMNS where trim(table_catalog)||'.'||trim(table_schema)||'.'||trim(table_name)='"""||   dbtbname ||"""_log_v' and column_name <> 'op'
      """) into cols;


--  DECLARE dbtbname STRING DEFAULT "project-id.test.debezium_usertable";
--  DECLARE pk STRING DEFAULT "ycsb_key";
  execute immediate format("""
      INSERT INTO """|| dbtbname ||""" ( """|| cols ||""")
      SELECT """|| cols ||""" 
      from
      (SELECT * , row_number() over(partition by """|| pk ||""" order  by source_ts DESC,pos DESC) row_num
      FROM """|| dbtbname ||"""_log_v
      where publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 2 DAY) 
      ) log_latest
      where row_num=1
      and op <> 'd';
  """);

      COMMIT TRANSACTION;
  
    EXCEPTION WHEN ERROR THEN
      -- Roll back the transaction inside the exception handler.
      SELECT @@error.message;
      ROLLBACK TRANSACTION;
    
    END;
  
END IF;

END;
