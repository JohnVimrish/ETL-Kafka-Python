from datetime import datetime as dt
from importcsv.csvutility import CSVUtility
from jsoncustom.fusiontableconfigparametervalue import TableConfigParametersValue
from jsoncustom.fusionconfigparametervalue import ConfigParametersValue
from database.dbinitialisers import DatabaseInitialiser
from root.fusioncommonvariables import CommonVariables
from util.fusionloggingutility import LoggingUtil


class JSONSQLExecution():

    def __init__(self,
                 table_name,
                 ObjLogging: LoggingUtil,
                 table_log_directory ,                                   
                 config_filepath,
                 **process_tables_info 
                 ):
        self.table_name = table_name
        self.table_log_path = f'{table_name}-log_file.log'
        self.JsonSqlExecutionLogger = ObjLogging.setup_logger(table_name, ConfigParametersValue.json_sql_exec_log_level)
        self.process_csv = False 
        if (process_tables_info.get(CommonVariables.process_csv) is not None and process_tables_info[CommonVariables.process_csv] is True ) :                   
                self.process_csvlist_based_ontables  = process_tables_info[CommonVariables.process_csv_table_csv_list]
                self.process_csv = True
        self.JsonSqlExecutionLoggerFile = ObjLogging.create_log_file(self.table_log_path,table_log_directory)
        self.table_log_path =  ConfigParametersValue.log_base_directory +table_log_directory+self.table_log_path
        self.ObjLogging = ObjLogging
        ObjLogging.link_logger_filehandler(self.JsonSqlExecutionLogger, self.JsonSqlExecutionLoggerFile)
        self.ObjTableConfiguration  = TableConfigParametersValue(config_filepath,self.ObjLogging,self.JsonSqlExecutionLoggerFile)

    def __concatenate_sql(self, concat_variable_first, concat_variable_sec):
        for string in concat_variable_first:  
            first_half_string = string
        for string in concat_variable_sec: 
            second_half_string = string
        return [' '.join([first_half_string, second_half_string])]


    def __replace_last_refresh_date (self,source_query :list , replace_value : str):
        return [''.join(source_query).format(replace_value)]


    def __execute_dft_queries(self, 
                            source_extract_query, 
                            target_executable_sql, 
                            read_batch_num, 
                            write_batch_num, 
                            src_connection_obj, 
                            tgt_connection_obj):

        for value in source_extract_query:
            input_query = value
        for value in target_executable_sql:
            execute_query = value
        src_connection_obj.set_read_array_size(read_batch_num)
        
        source_error_message = 'Error Code while executing source query : {} .\n Error Message while executing source query : {} .'
        self.JsonSqlExecutionLogger.info(f'Execution of Source query start time {dt.now()}')
        
        src_connection_obj.execute_sql(input_query, source_error_message)

        self.JsonSqlExecutionLogger.info(f'Execution of Source query end time {dt.now()}')

        batch_count = 0
        while True:
            
            self.JsonSqlExecutionLogger.debug(f'Batch Number :{batch_count} fetch starts at {dt.now()}')
            source_batch_result = src_connection_obj.fetch_in_batches( read_batch_num, source_error_message)
            self.JsonSqlExecutionLogger.debug( f'Batch Number :{batch_count} fetch ends at {dt.now()}')
            
            if len(source_batch_result) > 0:
                src_connection_obj.update_read_row_count()
                tgt_connection_obj.upsert_data_in_batches(source_batch_result, execute_query, write_batch_num, batch_count)
            else :
                self.JsonSqlExecutionLogger.info('Exited from reading in source as return result was 0 , on Read Batch Number :{} .'.format(batch_count))
                break

            batch_count += 1

        total_src_rows = src_connection_obj.get_full_read_row_count()
        total_tgt_rows = tgt_connection_obj.get_full_write_row_count()
        return total_src_rows, total_tgt_rows

    def __execute_dft_queries_csv(self, 
                            rearrage_csv_columns,
                            target_executable_sql, 
                            read_batch_num, 
                            write_batch_num, 
                            tgt_connection_obj):

        for value in target_executable_sql:
            execute_query = value
        total_src_rows  = 0
        for csv_list in self.process_csvlist_based_ontables :
            self.JsonSqlExecutionLogger.info(f'Execution of source  csv : {csv_list},read_time  start time {dt.now()}')              
            readcsvfileNbatches = CSVUtility.read_csv_as_batches(csv_list,read_batch_num,self.JsonSqlExecutionLogger,self.ObjTableConfiguration.data_flow_task_src_csv_dtypes)
            self.JsonSqlExecutionLogger.info(f'Execution of source  csv : {csv_list},read_time  end time {dt.now()}')
            batch_count = 0
            for  csv_batches in readcsvfileNbatches :
                    self.JsonSqlExecutionLogger.debug(f'Batch Number :{batch_count} column rearrange and conversion to tupple array starts at {dt.now()}')
                    total_src_rows +=CSVUtility.get_dataframe_row_count(csv_batches)
                    source_batch_result = CSVUtility.rearrange_columns_and_convert_to_tuple_array(
                         csv_batches,rearrage_csv_columns,self.JsonSqlExecutionLogger)
                    self.JsonSqlExecutionLogger.debug( f'Batch Number :{batch_count} column rearrange and conversion to tupple array ends at {dt.now()}')
                    tgt_connection_obj.upsert_data_in_batches(source_batch_result, execute_query, write_batch_num, batch_count)
                    self.JsonSqlExecutionLogger.debug( f'Current Batch records are successfully  uploaded to target table in DB {dt.now()}')
                    batch_count += 1
        
        total_tgt_rows = tgt_connection_obj.get_full_write_row_count()
        return total_src_rows, total_tgt_rows

    def __execute_queries_log(  self, 
                            activity, 
                            connection_obj, 
                            sql_list,
                            separator):
        self.JsonSqlExecutionLogger.info(f'Queries of  {activity} : {separator.join(sql_list)}')        
        self.__execute_queries( 
                            activity, 
                            connection_obj, 
                            sql_list)
        self.JsonSqlExecutionLogger.info(f'Queries with  {activity} Has Executed')

    def __execute_queries(  self, 
                            activity, 
                            connection_obj, 
                            sql_list
                            ):
        if len(sql_list) > 0 :
            for sql in sql_list:
                error_message = f'Error executing for activity :{activity} on query :{sql}, with' + ' error_message : {} '
                connection_obj.execute_sql(sql, error_message)
        
    def __pre_trigger_task(self, 
                            tgt_connection_obj, 
                            load_type: str):

        FULL = CommonVariables.full_load_type
        INCREMENTAL = CommonVariables.inc_load_type
        separator = CommonVariables.comma_seperator
        other_sql = self.ObjTableConfiguration.pre_other_target_sql 
        trunc_ods_tbl_sql = self.ObjTableConfiguration.pre_trun_target_full_sql
        drop_indexes_ods_sql = self.ObjTableConfiguration.pre_drop_target_ind_full_sql 
        trunc_ods_inc_sql = self.ObjTableConfiguration.pre_trun_target_inc_sql
        drop_indexes_inc_sql = self.ObjTableConfiguration.pre_drop_target_ind_inc_sql

        for activity in CommonVariables.pre_trigger_steps:
            
            if activity == CommonVariables.task_other_sql:
                self.__execute_queries_log(activity, tgt_connection_obj, other_sql,separator)                

            elif (activity == CommonVariables.task_truncate_tbl_full_sql and load_type == FULL):
                self.__execute_queries_log(activity, tgt_connection_obj, trunc_ods_tbl_sql, separator)

            elif (activity == CommonVariables.task_drp_indexes_full_sql and load_type == FULL):
                self.__execute_queries_log(activity, tgt_connection_obj, drop_indexes_ods_sql,separator)

            elif (activity == CommonVariables.task_truncate_tbl_inc_sql and load_type == INCREMENTAL):
                self.__execute_queries_log(activity, tgt_connection_obj, trunc_ods_inc_sql, separator)

            elif (activity == CommonVariables.task_drp_indexes_inc_sql and load_type == INCREMENTAL):
                self.__execute_queries_log(activity, tgt_connection_obj, drop_indexes_inc_sql,separator)

    def __data_flow_task(self, 
                        table_name, 
                        src_connection_obj,
                        tgt_connection_obj, 
                        load_type):

        FULL=CommonVariables.full_load_type
        INCREMENTAL=CommonVariables.inc_load_type
        
        last_refresh_dt_tmp  = self.ObjTableConfiguration.last_refresh_date
        pre_execute_source_tmp = self.ObjTableConfiguration.data_flow_task_preexecute_extract_full_sql
        full_source_select_sql_tmp = self.ObjTableConfiguration.data_flow_task_extract_full_sql
        inc_source_select_sql_tmp = self.ObjTableConfiguration.data_flow_task_extract_inc_sql
        process_csv_rearrage_column_tmp = self.ObjTableConfiguration.dft_source_csv_columns
        inc_target_upser_sql_tmp = self.ObjTableConfiguration.dft_insert_target_inc_sql
        post_execute_target_sql_full_tmp  = self.ObjTableConfiguration.data_flow_task_post_exec_full_sql
        post_execute_target_sql_inc_tmp  = self.ObjTableConfiguration.data_flow_task_post_exec_inc_sql

        last_refresh_dt = last_refresh_dt_tmp if last_refresh_dt_tmp else ''
        source_read_batch = self.ObjTableConfiguration.dft_read_csv_batches_count
        target_write_batch = self.ObjTableConfiguration.dft_write_target_batches_count 
        full_source_select_sql =  full_source_select_sql_tmp if full_source_select_sql_tmp else []
        inc_source_select_sql =   (self.__replace_last_refresh_date(inc_source_select_sql_tmp, last_refresh_dt)   if len(inc_source_select_sql_tmp
                                  ) > 0 else [] ) if inc_source_select_sql_tmp else []
        perform_transform_target =  self.ObjTableConfiguration.dft_perform_target_transf 
        process_csv_rearrage_column = process_csv_rearrage_column_tmp if process_csv_rearrage_column_tmp else []
        full_target_insert_sql = self.ObjTableConfiguration.dft_insert_target_full_sql
        increment_target_upsert_sql = inc_target_upser_sql_tmp if inc_target_upser_sql_tmp else []
        pre_execute_source_sql = pre_execute_source_tmp if pre_execute_source_tmp else []
        post_execute_target_sql  = (post_execute_target_sql_full_tmp if load_type == FULL else 
                                    post_execute_target_sql_inc_tmp) if (post_execute_target_sql_full_tmp is not None and 
                                                                           post_execute_target_sql_inc_tmp is not None ) else  []


        

        if perform_transform_target == 'N':
            full_target_insert_sql = self.__concatenate_sql (full_target_insert_sql, full_source_select_sql)

        self.JsonSqlExecutionLogger.info(f"Queries of  Pre-Execute of Source SQL in DFT : {''.join(pre_execute_source_sql)}")

        self.__execute_queries(CommonVariables.task_dft, src_connection_obj, pre_execute_source_sql)

        if len(pre_execute_source_sql) > 0:
            pre_source_sql_row_count = src_connection_obj.get_curr_exec_rowcount()
            self.JsonSqlExecutionLogger.info(f'Source PreSQL rowcount : {pre_source_sql_row_count}')

        self.JsonSqlExecutionLogger.info(f'DFT Load Type  : {load_type}')
        self.JsonSqlExecutionLogger.info(f'Read Batches Count  : {source_read_batch}')
        self.JsonSqlExecutionLogger.info(f'Write Batches Count  : {target_write_batch}')
        self.JsonSqlExecutionLogger.info(f"Full Load Source Extract SQL : {''.join(full_source_select_sql)}")
        self.JsonSqlExecutionLogger.info(f"Full Load Target Execute SQL : {''.join(full_target_insert_sql)}")
        self.JsonSqlExecutionLogger.info(f"Increment Load Source Extract SQL : {''.join(inc_source_select_sql)}")
        self.JsonSqlExecutionLogger.info(f"Increment Load Target Execute SQL : {''.join(increment_target_upsert_sql)}")
        self.JsonSqlExecutionLogger.info(f"Post Execute SQL after DFT on Target Table : {''.join(post_execute_target_sql)}.")
        self.JsonSqlExecutionLogger.info(f'Insert into {table_name} starts at {dt.now()},with Perform Transform Target as {perform_transform_target}')

        if load_type == FULL:

            if perform_transform_target != 'N':

                rowread_rowswritten = self.__execute_dft_queries(   full_source_select_sql, 
                                                                    full_target_insert_sql, 
                                                                    source_read_batch, 
                                                                    target_write_batch, 
                                                                    src_connection_obj, 
                                                                    tgt_connection_obj) if self.process_csv is False else self.__execute_dft_queries_csv(
                                                                process_csv_rearrage_column,
                                                                full_target_insert_sql,
                                                                source_read_batch,
                                                                target_write_batch,
                                                                tgt_connection_obj    
                                                                )
                src_row_count = rowread_rowswritten[0]
                tgt_row_count = rowread_rowswritten[1]

                self.JsonSqlExecutionLogger.info(f'Source Row count :{src_row_count}')

            elif perform_transform_target == 'N':
                self.__execute_queries( CommonVariables.task_dft, 
                                        tgt_connection_obj, 
                                        full_target_insert_sql)
                tgt_row_count = tgt_connection_obj.get_curr_exec_rowcount()

            self.JsonSqlExecutionLogger.info(f'Target Row count :{tgt_row_count}')
            self.JsonSqlExecutionLogger.info(f'Insert full into {table_name} ends at {dt.now()}, with Perform Transform Target as {perform_transform_target}')

        elif load_type == INCREMENTAL:

            self.JsonSqlExecutionLogger.info(f'Insert incrmental into {table_name} starts at {dt.now()}, with Perform Transform Target as {perform_transform_target}')
            rowread_rowswritten = self.__execute_dft_queries(   inc_source_select_sql, 
                                                                increment_target_upsert_sql,
                                                                source_read_batch,
                                                                target_write_batch,
                                                                src_connection_obj,
                                                                tgt_connection_obj) if self.process_csv is False else self.__execute_dft_queries_csv(
                                                                process_csv_rearrage_column,
                                                                increment_target_upsert_sql,
                                                                source_read_batch,
                                                                target_write_batch,
                                                                tgt_connection_obj    
                                                                )
            src_row_count = rowread_rowswritten[0]
            tgt_row_count = rowread_rowswritten[1]
            
            self.JsonSqlExecutionLogger.info(f'Source Row count :{src_row_count}')
            self.JsonSqlExecutionLogger.info(f'Target Row count :{tgt_row_count}')
            self.JsonSqlExecutionLogger.info(f'Insert incrmental into {table_name} ends at {dt.now()}, with Perform Transform Target as {perform_transform_target}')

        
        if len(post_execute_target_sql) > 0:
                self.__execute_queries( CommonVariables.task_dft, 
                                        tgt_connection_obj, 
                                        post_execute_target_sql)
                tgt_row_count = tgt_connection_obj.get_curr_exec_rowcount()
                self.JsonSqlExecutionLogger.info(f'Post DFT Execution SQL has changed : {tgt_row_count} rows .')


        self.ObjTableConfiguration.Update_Table_config_wLast_refresh_date()
        self.JsonSqlExecutionLogger.info('Table Config Json has been updated with Last refresh date.')

        return tgt_row_count

    def __delete_operation( self, 
                            src_connection_obj,
                            tgt_connection_obj,
                            activity,
                            truncate_sql,
                            extract_pk_source_sql,
                            insert_pk_into_target_sql,
                            backup_sql, 
                            delete_sql, 
                            insert_candidates_of_deletes, 
                            read_batch_num,
                            write_batch_num):

        def __truncate_del_process_tables(tgt_connection_obj, trunc_query_lst, activity):
            for sql in trunc_query_lst:
                error_message = 'Error executing for activity :{} on query :{}, with'.format(activity,sql) + ' error_message : {}.'
                tgt_connection_obj.execute_sql(sql, error_message)

        def __store_pk_from_source_in_target(   src_pk_query,
                                                insert_pk_query,
                                                read_batch_processing_num,
                                                write_batch_processing_num,
                                                src_connection_obj,
                                                tgt_connection_obj):

            src_connection_obj.set_read_array_size(read_batch_processing_num)

            src_err_msg = 'Error Code while executing source query : {} .\nError Message while executing source query : {} .'
            self.JsonSqlExecutionLogger.info(f'Execution of Source query start time ,{dt.now()}.')
            src_connection_obj.execute_sql(src_pk_query, src_err_msg)
            self.JsonSqlExecutionLogger.info(
                'Execution of Source  query end time ,{}.'.format(dt.now()))
            batch_count = 0
            while True:
                self.JsonSqlExecutionLogger.debug(f'Batch Number :{batch_count} fetch starts at {dt.now()}')
                src_batch_result = src_connection_obj.fetch_in_batches(read_batch_num, src_err_msg)
                self.JsonSqlExecutionLogger.debug(f'Batch Number :{batch_count} fetch ends at {dt.now()}')
                
                if len(src_batch_result) > 0 :
                    tgt_connection_obj.upsert_data_in_batches(   src_batch_result,
                                                                insert_pk_query,
                                                                write_batch_processing_num,
                                                                batch_count)
                else :
                    self.JsonSqlExecutionLogger.info(f'Exiting...Source last fetch Row count 0, Batch Number :{batch_count}')
                    break
                batch_count += 1

            src_row_count = src_connection_obj.get_full_read_row_count()
            self.JsonSqlExecutionLogger.info(f' Row count  of records inserted to _pk table :{ tgt_connection_obj.get_full_write_row_count()}')

        def __comparison_of_rec_within_ods_temp_tables( tgt_connection_obj,
                                                        backup_query,
                                                        delete_candidates_insert,
                                                        activity):

            def __delete_from_ods_table(del_query):
                    error_message = 'Error executing for activity :{} on query :{}, with'.format(activity, del_query) + ' error_message : {} .'
                    tgt_connection_obj.execute_sql(del_query, error_message)

            error_message = 'Error executing for activity :{} on query :{}, with'.format(activity, backup_query) + ' error_message : {} .'
            tgt_connection_obj.execute_sql(backup_query, error_message)
            deleted_records =  tgt_connection_obj.get_full_write_records ()
            if len (deleted_records) > 0 :
                tgt_connection_obj.upsert_deleted_data(deleted_records,delete_candidates_insert)
                __delete_from_ods_table( delete_query)
            else :
                self.JsonSqlExecutionLogger.info(' Number of Deleted  records from ODS Table is 0 .')



        truncate_del_tbls_sql = truncate_sql
        for query in extract_pk_source_sql:
            source_input_query = query
        for query in insert_pk_into_target_sql:
            tgt_execute_query = query
        for query in backup_sql:
            del_backup_query = query
        for query in insert_candidates_of_deletes:
            candidates_for_delete_insert = query
        for query in delete_sql:
            delete_query = query



        __truncate_del_process_tables(  tgt_connection_obj, 
                                        truncate_del_tbls_sql,
                                        activity)
        if   self.process_csv is not True   :                     
            __store_pk_from_source_in_target(   source_input_query, 
                                                tgt_execute_query,
                                                read_batch_num,
                                                write_batch_num,
                                                src_connection_obj,
                                                tgt_connection_obj)

        __comparison_of_rec_within_ods_temp_tables( tgt_connection_obj,
                                                    del_backup_query,
                                                    candidates_for_delete_insert,
                                                    activity)



    def __delete_task(  self,
                        src_connection_obj,
                        tgt_connection_obj,
                        config_obj,
                        activity):
        self.JsonSqlExecutionLogger.info( f'Steps under {activity} will be  Executed')
        source_read_batch = self.ObjTableConfiguration.del_read_target_batches_count
        target_write_batch = self.ObjTableConfiguration.del_write_target_batches_count
        source_pk_extract_sql = self.ObjTableConfiguration.del_extract_primary_keys
        insert_pk_sql = self.ObjTableConfiguration.del_insert_primary_keys
        truncate_process_used_tbls = self.ObjTableConfiguration.del_trun_del_table_sql
        insert_identified_del_key_sql = self.ObjTableConfiguration.del_insert_del_table_sql  
        backup_sql = self.ObjTableConfiguration.del_insert_backup_sql
        delete_sql = self.ObjTableConfiguration.del_hard_delete_sql 

        self.__delete_operation(
                                src_connection_obj,
                                tgt_connection_obj,
                                activity,
                                truncate_process_used_tbls,
                                source_pk_extract_sql,
                                insert_pk_sql,
                                backup_sql,
                                delete_sql,
                                insert_identified_del_key_sql,
                                source_read_batch,
                                target_write_batch)


    def __post_trigger_task(self, 
                            tgt_connection_obj,
                            load_type):

        FULL = CommonVariables.full_load_type
        INCREMENTAL = CommonVariables.inc_load_type
        separator = CommonVariables.comma_seperator

        create_indexes_on_ODS_full = self.ObjTableConfiguration.post_create_ind_target_full_sql
        create_indexes_on_ODS_inc = self.ObjTableConfiguration.post_create_ind_target_incremental_sql
        other_sql = self.ObjTableConfiguration.post_other_target_sql

        for activity in CommonVariables.post_trigger_steps:

            if (activity == CommonVariables.task_create_indexes_full  and load_type == FULL): 
                self.__execute_queries_log( activity, 
                                        tgt_connection_obj,
                                        create_indexes_on_ODS_full,
                                        separator)

            elif (activity == CommonVariables.task_create_indexes_inc and load_type == INCREMENTAL):
                self.__execute_queries_log(activity,
                                        tgt_connection_obj,
                                        create_indexes_on_ODS_inc,
                                        separator)

            elif activity == CommonVariables.task_other_target_sql:
                self.__execute_queries_log(activity,
                                        tgt_connection_obj,
                                        other_sql,
                                        separator)

    def __analyse_task(self,
                        tgt_connection_obj,
                        activity):

        analyse_sql =self.ObjTableConfiguration.analyse_target_table_sql

        self.__execute_queries( activity,
                                tgt_connection_obj,
                                analyse_sql)

    def __execute_load_plan(self,
                            target_table_name,
                            load_type, 
                            src_connection_obj, 
                            tgt_connection_obj):


        delete_flag = self.ObjTableConfiguration.del_enable_flag
        analyse_flag = self.ObjTableConfiguration.analyse_flag

        output_result = dict()

        for task in CommonVariables.execution_steps:

            if task == CommonVariables.pre_trigger_task:
                self.JsonSqlExecutionLogger.info( f'Steps under {task} will be  Executed')
                self.__pre_trigger_task(
                                        tgt_connection_obj,
                                        load_type)

            elif (task == CommonVariables.data_flow_task and self.ObjTableConfiguration.data_flow_task is not None):
                self.JsonSqlExecutionLogger.info( f'Steps under {task} will be  Executed')
                tgt_row_count = self.__data_flow_task(  target_table_name,
                                                        src_connection_obj,
                                                        tgt_connection_obj,
                                                        load_type)

            elif (task == CommonVariables.delete_task and delete_flag == CommonVariables.delete_flag_true):
                self.JsonSqlExecutionLogger.info(f'Steps under {task} will be  Executed')
                self.__delete_task( src_connection_obj,
                                    tgt_connection_obj,
                                    task)

            elif task == CommonVariables.post_trigger_task:
                self.JsonSqlExecutionLogger.info(f'Steps under {task} will be  Executed')
                self.__post_trigger_task(   tgt_connection_obj,
                                            load_type)

            elif (task == CommonVariables.analyse_task and analyse_flag == CommonVariables.analyse_flag_true):
                self.JsonSqlExecutionLogger.info(f'Steps under {task} will be  Executed')
                self.__analyse_task(tgt_connection_obj,
                                    task)

        if tgt_connection_obj.cursor_status():
            output_result[CommonVariables.load_type] = load_type
            output_result[CommonVariables.result_of_load] = CommonVariables.status_failed
            output_result[CommonVariables.error_reason] = 'Cursor closed and not allowing other queries to be executed.'
            output_result[CommonVariables.num_of_rows_processed] = 0
        else:
            output_result[CommonVariables.load_type] = load_type
            output_result[CommonVariables.result_of_load] = CommonVariables.status_successfull
            output_result[CommonVariables.num_of_rows_processed] = tgt_row_count if self.ObjTableConfiguration.data_flow_task is not None else 0

        return output_result

    def perform_etl(    self, 
                        load_type, 
                        db_obj:DatabaseInitialiser, 
                        ):

        start_time = dt.now()
        self.JsonSqlExecutionLogger.info(
            'Start_time of ETL for {} ,{}'.format(self.table_name, start_time))

        return_variable = dict()

        try:
            src_connection = self.ObjTableConfiguration.source_connection
            tgt_connection = self.ObjTableConfiguration.target_connection
            tgt_schema_name = self.ObjTableConfiguration.target_schema_name
            tgt_tbl_name = self.ObjTableConfiguration.target_table_name
            try:
                if (self.process_csv is True and src_connection  == 'CSV') :
                   src_connection_obj = 'CSV'
                   tgt_connection_obj = db_obj.init_db_connection(
                                                                tgt_connection, 
                                                                self.JsonSqlExecutionLogger, 
                                                                self.JsonSqlExecutionLoggerFile, 
                                                                self.ObjLogging,
                                                                self.table_name)
                else :
                    src_connection_obj, tgt_connection_obj = db_obj.init_src_tgt_connections(
                                                                src_connection, 
                                                                tgt_connection, 
                                                                self.JsonSqlExecutionLogger, 
                                                                self.JsonSqlExecutionLoggerFile, 
                                                                self.ObjLogging,
                                                                self.table_name)
            except:

                error_dict = dict()
                error_dict[CommonVariables.load_type] = load_type
                error_dict[CommonVariables.result_of_load] = CommonVariables.status_failed
                error_dict[CommonVariables.table_schema] = tgt_schema_name
                error_dict[CommonVariables.load_start_time] = start_time
                error_dict[CommonVariables.error_reason] = 'Connection cannot be established as requested connection in config.json is missing from program\'s connection parameter.json'
                error_dict[CommonVariables.load_end_time] = dt.now()
                error_dict[CommonVariables.num_of_rows_processed] = 0
                error_dict[CommonVariables.table_name] = tgt_tbl_name
                error_dict[CommonVariables.log_loc] =self.table_log_path
                if (self.process_csv is True):
                    error_dict[CommonVariables.csv_files] = self.process_csvlist_based_ontables
                return_variable[self.table_name] = error_dict
            else:
                # starts executing tasks from packages
                try:
                    result = self.__execute_load_plan( tgt_tbl_name, 
                                                        load_type,
                                                        src_connection_obj,
                                                        tgt_connection_obj)
                except Exception as exception:
                    self.JsonSqlExecutionLogger.error(f'Error has been raised : {exception}', exc_info=True)
                    result = dict ()
                    result[CommonVariables.load_type] = load_type
                    result[CommonVariables.result_of_load] = CommonVariables.status_failed
                    result[CommonVariables.error_reason] = exception
                    result[CommonVariables.num_of_rows_processed] = 0

                result[CommonVariables.table_schema] = tgt_schema_name
                result[CommonVariables.load_start_time] = start_time
                result[CommonVariables.table_name] = tgt_tbl_name
                result[CommonVariables.load_end_time] = dt.now()
                result[CommonVariables.log_loc] =self.table_log_path
                if (self.process_csv is True):
                    result[CommonVariables.csv_files] = self.process_csvlist_based_ontables
                return_variable[self.table_name] = result

            db_obj.release_connections( src_connection_obj,
                                            tgt_connection_obj) if self.process_csv is False else db_obj.release_connection(tgt_connection_obj)
                

        except Exception as exc:
            self.JsonSqlExecutionLogger.error(
                'Error has been raised : {} .'.format(exc), exc_info=True)
            error_dict = dict()
            error_dict[CommonVariables.load_type] = load_type
            error_dict[CommonVariables.table_schema] = ''
            error_dict[CommonVariables.load_start_time] = start_time
            error_dict[CommonVariables.load_end_time] = dt.now()
            error_dict[CommonVariables.result_of_load] = CommonVariables.status_failed
            error_dict[CommonVariables.error_reason] = exc
            error_dict[CommonVariables.num_of_rows_processed] = 0
            error_dict[CommonVariables.table_name] = tgt_tbl_name
            error_dict[CommonVariables.log_loc] =self.table_log_path
            return_variable[self.table_name] = error_dict
            
        self.JsonSqlExecutionLogger.info(f'End_time of ETL for  {self.table_name} , {dt.now()}')

        return return_variable
