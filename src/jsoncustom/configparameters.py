from jsoncustom.jsontagvariables import JsonTagVariables
from jsoncustom.jsonvalueextract import JsonValueExtractor
from util.stringutil import StringUtility

class ConfigParametersValue():

    @classmethod
    def __init__(cls, main_config_obj,
                      dbconnection_config_obj,
                      log_config_obj,
                      table_config_obj,
                      kafka_config_obj):

        cls.max_threads_count_process                       = main_config_obj.json_val(JsonTagVariables.max_threads_count_process)
        cls.load_type                                       = main_config_obj.json_val(JsonTagVariables.type_of_load_to_process)
        cls.table_configuration_file_directory              = StringUtility.derive_actual_directory(main_config_obj.json_val(JsonTagVariables.table_configuration_file_directory))
        cls.log_base_directory                              = StringUtility.derive_actual_directory(main_config_obj.json_val(JsonTagVariables.log_base_directory))
        cls.main_log_file_name_format                       = main_config_obj.json_val(JsonTagVariables.log_main_file_name_format)
        cls.smtp_server_host                                = main_config_obj.json_val(JsonTagVariables.email_server_host)
        cls.smtp_server_port                                = main_config_obj.json_val(JsonTagVariables.email_server_port)
        cls.email_sender_emailid                            = main_config_obj.json_val(JsonTagVariables.email_sender_emailid)
        cls.email_receiver_list                             = main_config_obj.json_val(JsonTagVariables.email_receiver_list)
        cls.email_subjectline                               = main_config_obj.json_val(JsonTagVariables.email_subjectline)
        cls.dbconnectors                                    = dbconnection_config_obj.json_val(JsonTagVariables.connectors)
        cls.all_packages_log_level                          = log_config_obj.json_val(JsonTagVariables.allpackages_log_level)
        cls.kafka_main_log_level                            = log_config_obj.json_val(JsonTagVariables.kafka_main_log_level)
        cls.thread_exec_log_level                           = log_config_obj.json_val(JsonTagVariables.thread_exec_log_level)
        cls.db_initialiser_log_level                        = log_config_obj.json_val(JsonTagVariables.db_initialiser_log_level)
        cls.json_value_extc_log_level                       = log_config_obj.json_val(JsonTagVariables.json_value_extc_log_level)
        cls.postgres_connection_pool_log_level              = log_config_obj.json_val(JsonTagVariables.postgres_connection_pool_log_level)
        cls.postgres_connector_log_level                    = log_config_obj.json_val(JsonTagVariables.postgres_connector_log_level)
        cls.kakfa_etl_log_level                             = log_config_obj.json_val(JsonTagVariables.kakfa_etl_log_level)
        cls.kafka_connect_log_level                         = log_config_obj.json_val(JsonTagVariables.kafka_connect_log_level)
        cls.table_groups                                    = table_config_obj.json_val(JsonTagVariables.sil_table_root_tag)
        cls.kafka_bootstrap_servers                         = kafka_config_obj.json_val(JsonTagVariables.kafka_bootstrap_servers)
        cls.default_consumer_group_id                       = kafka_config_obj.json_val(JsonTagVariables.default_consumer_group_id)
        cls.session_timeout_inms                            = kafka_config_obj.json_val(JsonTagVariables.session_timeout_inms)
        cls.default_offset_reset                            = kafka_config_obj.json_val(JsonTagVariables.default_offset_reset)
        cls.exclude_topics                                  = kafka_config_obj.json_val(JsonTagVariables.exclude_topics)
        cls.processing_time_for_each_topic_inms             = kafka_config_obj.json_val(JsonTagVariables.processing_time_for_each_topic_inms)
        cls.consumer_consuming_timeout_inms                 = kafka_config_obj.json_val(JsonTagVariables.consumer_consuming_timeout_inms)
        cls.enable_auto_commit                              = kafka_config_obj.json_val(JsonTagVariables.enable_auto_commit)
        cls.max_poll_interval_inms                          = kafka_config_obj.json_val(JsonTagVariables.max_poll_interval_inms)
        cls.consumer_live_status_interval_ins               = kafka_config_obj.json_val(JsonTagVariables.consumer_live_status_interval_ins)

class TableConfigParametersValue():

    def __init__(self, table_config_path,ObjLogger,Logfile):

        self.ObjTableConfiguration                                   = JsonValueExtractor(table_config_path,Logfile,ConfigParametersValue.json_value_extc_log_level)
        self.target_connection                                       = self.ObjTableConfiguration.json_val(JsonTagVariables.target_db)
        self.target_table_name                                       = self.ObjTableConfiguration.json_val(JsonTagVariables.target_tbl)
        self.target_schema_name                                      = self.ObjTableConfiguration.json_val(JsonTagVariables.target_schema)
        self.pre_other_target_sql                                    = self.ObjTableConfiguration.json_val(JsonTagVariables.pre_trig_other_sql)
        self.pre_trun_target_full_sql                                = self.ObjTableConfiguration.json_val(JsonTagVariables.pre_trig_trunc_ods_full)
        self.pre_drop_target_ind_full_sql                            = self.ObjTableConfiguration.json_val(JsonTagVariables.pre_trig_drop_indexes_full)
        self.pre_drop_target_ind_inc_sql                             = self.ObjTableConfiguration.json_val(JsonTagVariables.pre_trig_drop_indexes_inc)
        self.data_flow_task                                          = self.ObjTableConfiguration.json_val(JsonTagVariables.dft_task)
        self.dft_write_target_batches_count                          = self.ObjTableConfiguration.json_val(JsonTagVariables.dft_target_write_batches_num)
        self.dft_perform_target_transf                               = self.ObjTableConfiguration.json_val(JsonTagVariables.dft_perform_transform_target)
        self.dft_insert_target_full_sql                              = self.ObjTableConfiguration.json_val(JsonTagVariables.dft_insert_full_sql)
        self.dft_insert_target_inc_sql                               = self.ObjTableConfiguration.json_val(JsonTagVariables.dft_upsert_inc_sql)
        # self.data_flow_task_src_csv_dtypes                           = self.ObjTableConfiguration.json_val(JsonTagVariables.data_flow_task_src_csv_dtypes)
        self.del_hard_delete_sql                                     = self.ObjTableConfiguration.json_val(JsonTagVariables.del_process_tbls)
        self.post_other_target_sql                                   = self.ObjTableConfiguration.json_val(JsonTagVariables.post_trig_other_sql)
        self.post_create_ind_target_full_sql                         = self.ObjTableConfiguration.json_val(JsonTagVariables.post_trig_create_indexes_full)
        self.post_create_ind_target_incremental_sql                  = self.ObjTableConfiguration.json_val(JsonTagVariables.post_trig_create_indexes_inc)

