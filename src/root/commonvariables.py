class CommonVariables () :

    # variables to store information of log level for each modules 
    thread_log_folder = str()
    etl_project_directory = str()
    log_levels            = dict()
    comma_seperator = ','
        
    # dft in clause check parameters 
    values          = 'VALUES_CLAUSE'
    extract_full    = 'EXTRACT_TGT_FULL_SQL'
    extract_inc     = 'INSERT_TGT_FULL_SQL'

    # package ETL_to_Postgres static variables 
    execution_steps                = ['PRE_TRIGGER_TASK','DATA_FLOW_TASK','DELETE_TASK','POST_TRIGGER_TASK']
    pre_trigger_steps              = ['OTHER_TGT_SQL','TRUNCATE_TGT_TABLE_FULL_SQL','DROP_TGT_INDEXES_FULL_SQL','TRUNCATE_TGT_TABLE_INC_SQL','DROP_TGT_INDEXES_INC_SQL']
    post_trigger_steps             = ['CREATE_INDEX_TGT_FULL_SQL','CREATE_INDEX_TGT_INC_SQL','OTHER_TGT_SQL']
    full_load_type                 = 'FULL'
    inc_load_type                  = 'INCREMENTAL'
    last_refresh_dt_not_present    = 'Not  Present'
    
    # log variable descriptions 
    load_type                   = 'LOAD_TYPE'
    result_of_load              = 'LOAD_RESULT'
    log_loc                     = 'LOG_LOCATION'
    error_reason                = 'LOAD_FAILURE_CAUSE'
    num_of_rows_processed       = 'ROWS_PROCESSED_BYDFT'
    successfull_load            = 'SUCCESSFUL'
    failed_load                 = 'FAILED'
    table_schema                = 'TABLE_SCHEMA'
    table_name                  = 'TABLE_NAME'
    load_start_time             = 'LOAD_START_TIME'
    load_end_time               = 'LOAD_END_TIME'

    # lof file path 
    main_log_file_path                         = '/main_log_file.log'

    
    # each tasks
    pre_trigger_task                            = 'PRE_TRIGGER_TASK'
    data_flow_task                              = 'DATA_FLOW_TASK'
    post_trigger_task                           = 'POST_TRIGGER_TASK'
    task_other_sql                              = 'OTHER_SQL'
    task_other_target_sql                       = 'OTHER_TGT_SQL'
    task_dft                                    = 'Task_DFT'
    task_truncate_tbl_full_sql                  = 'TRUNCATE_TGT_TABLE_FULL_SQL'
    task_truncate_tbl_inc_sql                   = 'TRUNCATE_TGT_TABLE_INC_SQL'
    task_drp_indexes_full_sql                   = 'DROP_TGT_INDEXES_FULL_SQL' 
    task_drp_indexes_inc_sql                    = 'DROP_TGT_INDEXES_INC_SQL'
    task_create_indexes_full                    = 'CREATE_INDEX_TGT_FULL_SQL'  
    task_create_indexes_inc                     = 'CREATE_INDEX_TGT_INC_SQL'
    

    # logger_name module vise 
    main_config                                 = 'main'
    connection_param                            = 'connection_param_config'  
    js_group_exec                               = 'json_group_execution'
    db_init                                     = 'DatabaseInitialiser'
    postgres_con_pool                           = 'PostgresConnectionPool'
    thread_exec                                 = 'ThreadExecution'
    log_level_config                            = 'logLevelConfig'
    table_config                                = ''
    Kakfa_etl_config                            = 'Kafka_ETL_Process'
    kafka_connect_config                        = 'Kafka_CDC_Consumer'
    Postgres_Database                           = 'POSTGRESQL'

    offset_reset_full                           = 'earliest'
    offset_reset_inc                            = 'latest'
    thread_prefix                               = 'kafka_etl'
