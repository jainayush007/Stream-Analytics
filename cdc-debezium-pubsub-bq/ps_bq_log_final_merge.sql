DECLARE region STRING DEFAULT 'region-{{ params.region }}';
DECLARE sp_db STRING DEFAULT '{{ params.sp_db }}';
DECLARE dbtbname STRING DEFAULT '{{ params.dbtbname }}';
DECLARE pk STRING DEFAULT '{{ params.pk }}'; --DEFAULT "trans_id,cc_number";
DECLARE colname STRING; 


IF (select pk) = 'retrieve'
THEN
    execute immediate format("""  
    select distinct JSON_VALUE(clean_attributes.schema.fields[0].field) as pk
    from (SELECT publish_time, SAFE.PARSE_JSON(JSON_EXTRACT_SCALAR(attributes.ordering_key)) clean_attributes FROM """||dbtbname||"""_log )
    where publish_time = (select max(publish_time) from """||dbtbname||"""_log );
    """) into pk;
END IF;

execute immediate format("""  
select string_agg(fields.col) from
    (SELECT DISTINCT
    CASE WHEN field= '"""||pk||"""' THEN 'CAST(COALESCE(JSON_VALUE(data.payload.after.'|| field ||'),JSON_VALUE(data.payload.before.'|| field || ')) AS '|| type || ') '|| field
                                ELSE  'CAST(JSON_VALUE(data.payload.after.'|| field ||') AS ' || type || ') ' || field
    END col
    from
        (SELECT JSON_VALUE(x, '$.field') field,
        CASE WHEN JSON_VALUE(x, '$.name') = 'io.debezium.time.ZonedTimestamp' THEN 'timestamp' 
            WHEN JSON_VALUE(x, '$.type') ='int32' THEN 'int64'
            WHEN JSON_VALUE(x, '$.type') ='float' THEN 'float64'
            ELSE JSON_VALUE(x, '$.type') END type
        --,coalesce('"""||pk||"""',JSON_VALUE(clean_attributes.schema.fields[0].field)) as pk
        FROM
         (SELECT *, SAFE.PARSE_JSON(JSON_EXTRACT_SCALAR(attributes.ordering_key)) clean_attributes FROM """||dbtbname||"""_log )
        --, `ayush-agrolis.curated.source_metadata` meta
        ,unnest(JSON_QUERY_ARRAY(`data`, '$.schema.fields[0].fields')) x
        where publish_time = (select max(publish_time) from """||dbtbname||"""_log )
        and JSON_VALUE(x, '$.field')<>'bq_load_ts'
        ) tbl
    ) fields
""") into colname;


SET @@dataset_id = sp_db;

CALL sp_debezium_log_merge(dbtbname,colname,pk,region);