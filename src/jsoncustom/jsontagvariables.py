class JsonTagVariables():

    # config file parameters
    max_threads_count_process='MAX_THREADS_PROCESS'
    type_of_load_to_process='LOAD_TYPE'
    table_configuration_file_directory='CONFIG_FILE_DIR'
    log_base_directory = 'LOG_BASE_DIR'
    log_main_file_name_format ='LOG_MAIN_FILE_NAME_FORMAT'
    email_server_host='EMAIL_SMTP_SERVER_HOST'
    email_server_port='EMAIL_SMTP_SERVER_PORT'
    email_sender_emailid='EMAIL_SENDER_EMAIL_ID'
    email_receiver_list='EMAIL_RECEIVER_EMAIL_ID'
    email_subjectline='EMAIL_SUBJECT'
    wait_time_when_no_requestid_found='WAIT_MINUTES_NO_REQUEST'

    # Connection  Parameters File
    connectors = 'Database_Connectors'
    user_id = 'user_id'
    password = 'password'
    host = 'host'
    port = 'port'
    database = 'database'
    min_connection_pools = 'max_num_connections'
    max_connection_pools = 'max_num_connections'
    increment_connection = 'increment_connections'
    database_type = 'db_type'
    connection_pool = 'connection_pool'
    database_prod = 'database_product'
    enable_connection = 'enable_connection_pool_flg'

    # Log level config file details
    allpackages_log_level  = 'LOG_LEVEL'
    kafka_main_log_level='LOG_LEVEL--KAFKA_MAIN'
    thread_exec_log_level='LOG_LEVEL--THREAD_EXECUTION'
    db_initialiser_log_level='LOG_LEVEL--DB_INITIALISERS'
    json_value_extc_log_level='LOG_LEVEL--JSON_VALUE_EXTRACT'
    postgres_connection_pool_log_level='LOG_LEVEL--POSTGRES_CONNECTION_POOL'
    postgres_connector_log_level='LOG_LEVEL--POSTGRES_CONNECTOR'
    json_sql_value_extc_log_level='LOG_LEVEL--JSON_SQL_EXECUTION'
    kakfa_etl_log_level = 'LOG_LEVEL--KAKFA_ETL_CONFIG'
    kafka_connect_log_level = 'LOG_LEVEL--KAFKA_CONNECT_CONFIG'	 
       
    # kafka configuration tag variables :
    kafka_bootstrap_servers = 'KAFKA_BOOTSTRAP_SERVERS'
    default_consumer_group_id = 'DEFAULT_CONSUMER_GROUP_ID'
    session_timeout_inms = 'SESSION_TIMEOUT_INMS'
    default_offset_reset = 'DEFAULT_OFFSET_RESET'
    exclude_topics = 'EXCLUDE_TOPICS'
    processing_time_for_each_topic_inms = 'PROCESSING_TIME_FOR_EACH_TOPIC_INMS'
    consumer_consuming_timeout_inms = 'CONSUMER_CONSUMING_TIMEOUT_INMS'
    enable_auto_commit = 'ENABLE_AUTO_COMMIT'
    max_poll_interval_inms = 'MAX_POLL_INTERVAL_INMS'
    consumer_live_status_interval_ins ='CONSUMER_LIVE_STATUS_INTERVAL_INS'
    group_id  = 'group.id'
    auto_offset_reset = 'auto.offset.reset'

    # SIL Tables config file details
    sil_table_root_tag='TABLE_GROUPS'
    list_of_tables_to_process='TABLES'
    load_type='LOAD_TYPE'
    table_categorisation='GROUP'
    max_thread_spin  = 'MAX_THREADS_SIL_PROCESS'

    # global json variables which has the path  to extract data from each table's associated config file
    refresh_dt                        = 'LAST_REFRESH_DT'
    target_db                         = 'TGT_CONNECTION'
    target_schema                     = 'TGT_SCHEMA_NAME'
    target_tbl                        = 'TGT_TABLE_NAME'
    pre_trig_other_sql                = 'PRE_TRIGGER_TASK--OTHER_TGT_SQL'
    pre_trig_trunc_ods_full           = 'PRE_TRIGGER_TASK--TRUNCATE_TGT_TABLE_FULL_SQL'
    pre_trig_trunc_ods_inc            = 'PRE_TRIGGER_TASK--TRUNCATE_TGT_TABLE_INC_SQL'
    pre_trig_drop_indexes_full        = 'PRE_TRIGGER_TASK--DROP_TGT_INDEXES_FULL_SQL'
    pre_trig_drop_indexes_inc         = 'PRE_TRIGGER_TASK--DROP_TGT_INDEXES_INC_SQL'
    dft_task                          = 'DATA_FLOW_TASK'
    dft_target_write_batches_num      = 'DATA_FLOW_TASK--WRITE_TGT_BATCHES'
    dft_perform_transform_target      = 'DATA_FLOW_TASK--PERFORM_TGT_TRANSFORM'
    dft_values_clause                 = 'DATA_FLOW_TASK--VALUES_CLAUSE'
    dft_insert_full_sql               = 'DATA_FLOW_TASK--INSERT_TGT_FULL_SQL'
    dft_upsert_inc_sql                = 'DATA_FLOW_TASK--INSERT_TGT_INC_SQL'
    del_process_tbls                  = 'DELETE_TASK--DELETE_SQL'
    post_trig_other_sql               = 'POST_TRIGGER_TASK--OTHER_TGT_SQL'
    post_trig_create_indexes_full     = 'POST_TRIGGER_TASK--CREATE_INDEX_TGT_FULL_SQL'
    post_trig_create_indexes_inc      = 'POST_TRIGGER_TASK--CREATE_INDEX_TGT_INC_SQL'
