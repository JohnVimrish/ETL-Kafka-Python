from datetime import datetime as dt
from jsoncustom.configparameters import TableConfigParametersValue,ConfigParametersValue
from database.dbinitialisers import DatabaseInitialiser
from root.commonvariables import CommonVariables
from util.loggingutility import LoggingUtil


class SQLExecution():

    def __init__(self,
                 table_name,
                 ObjLogging: LoggingUtil,
                 table_log_file ,                                   
                 config_filepath):
        self.table_name = table_name
        self.SqlExecutionLogger = ObjLogging.setup_logger(table_name, ConfigParametersValue.json_sql_exec_log_level)
        self.ObjLogging = ObjLogging
        self.SqlExecutionLoggerFile = table_log_file
        ObjLogging.link_logger_filehandler(self.SqlExecutionLogger, self.SqlExecutionLoggerFile)
        self.ObjTableConfiguration  = TableConfigParametersValue(config_filepath,self.ObjLogging,self.SqlExecutionLoggerFile)

    def __concatenate_sql(self, concat_variable_first, concat_variable_sec):
        for string in concat_variable_first:  
            first_half_string = string
        for string in concat_variable_sec: 
            second_half_string = string
        return [' '.join([first_half_string, second_half_string])]

    def __execute_dft_queries(self, 
                            rearrage_csv_columns,
                            target_executable_sql, 
                            read_batch_num, 
                            write_batch_num, 
                            tgt_connection_obj):

            for value in target_executable_sql:
                execute_query = value
            total_src_rows  = 0
            for csv_list in self.process_csvlist_based_ontables :
                self.SqlExecutionLogger.info(f'Execution of source  csv : {csv_list},read_time  start time {dt.now()}')              
                readcsvfileNbatches = CSVUtility.read_csv_as_batches(csv_list,read_batch_num,self.SqlExecutionLogger,self.ObjTableConfiguration.data_flow_task_src_csv_dtypes)
                self.SqlExecutionLogger.info(f'Execution of source  csv : {csv_list},read_time  end time {dt.now()}')
                batch_count = 0
                for  csv_batches in readcsvfileNbatches :
                        self.SqlExecutionLogger.debug(f'Batch Number :{batch_count} column rearrange and conversion to tupple array starts at {dt.now()}')
                        total_src_rows +=CSVUtility.get_dataframe_row_count(csv_batches)
                        source_batch_result = CSVUtility.rearrange_columns_and_convert_to_tuple_array(
                            csv_batches,rearrage_csv_columns,self.SqlExecutionLogger)
                        self.SqlExecutionLogger.debug( f'Batch Number :{batch_count} column rearrange and conversion to tupple array ends at {dt.now()}')
                        tgt_connection_obj.upsert_data_in_batches(source_batch_result, execute_query, write_batch_num, batch_count)
                        self.SqlExecutionLogger.debug( f'Current Batch records are successfully  uploaded to target table in DB {dt.now()}')
                        batch_count += 1
            
            total_tgt_rows = tgt_connection_obj.get_full_write_row_count()
            return total_src_rows, total_tgt_rows


    def __execute_queries_log(  self, 
                            activity, 
                            connection_obj, 
                            sql_list):
        separator = CommonVariables.comma_seperator
        self.SqlExecutionLogger.info(f'Queries of  {activity} : {separator.join(sql_list)}')        
        self.__execute_queries( 
                            activity, 
                            connection_obj, 
                            sql_list)
        self.SqlExecutionLogger.info(f'Queries with  {activity} Has Executed')

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
        other_sql = self.ObjTableConfiguration.pre_other_target_sql 
        trunc_ods_tbl_sql = self.ObjTableConfiguration.pre_trun_target_full_sql
        drop_indexes_ods_sql = self.ObjTableConfiguration.pre_drop_target_ind_full_sql 
        trunc_ods_inc_sql = self.ObjTableConfiguration.pre_trun_target_inc_sql
        drop_indexes_inc_sql = self.ObjTableConfiguration.pre_drop_target_ind_inc_sql

        for activity in CommonVariables.pre_trigger_steps:
            
            if activity == CommonVariables.task_other_sql:
                self.__execute_queries_log(activity, tgt_connection_obj, other_sql)                

            elif (activity == CommonVariables.task_truncate_tbl_full_sql and load_type == FULL):
                self.__execute_queries_log(activity, tgt_connection_obj, trunc_ods_tbl_sql)

            elif (activity == CommonVariables.task_drp_indexes_full_sql and load_type == FULL):
                self.__execute_queries_log(activity, tgt_connection_obj, drop_indexes_ods_sql)

            elif (activity == CommonVariables.task_truncate_tbl_inc_sql and load_type == INCREMENTAL):
                self.__execute_queries_log(activity, tgt_connection_obj, trunc_ods_inc_sql)

            elif (activity == CommonVariables.task_drp_indexes_inc_sql and load_type == INCREMENTAL):
                self.__execute_queries_log(activity, tgt_connection_obj, drop_indexes_inc_sql)

    def __data_flow_task(self, 
                        table_name, 
                        tgt_connection_obj, 
                        load_type):

        FULL=CommonVariables.full_load_type
        INCREMENTAL=CommonVariables.inc_load_type
        
        process_csv_rearrage_column_tmp = self.ObjTableConfiguration.dft_source_csv_columns
        inc_target_upser_sql_tmp = self.ObjTableConfiguration.dft_insert_target_inc_sql
        source_read_batch = self.ObjTableConfiguration.dft_read_csv_batches_count
        target_write_batch = self.ObjTableConfiguration.dft_write_target_batches_count 

        perform_transform_target =  self.ObjTableConfiguration.dft_perform_target_transf 
        process_csv_rearrage_column = process_csv_rearrage_column_tmp if process_csv_rearrage_column_tmp else []
        full_target_insert_sql = self.ObjTableConfiguration.dft_insert_target_full_sql
        increment_target_upsert_sql = inc_target_upser_sql_tmp if inc_target_upser_sql_tmp else []

        self.SqlExecutionLogger.info(f'DFT Load Type  : {load_type}')
        self.SqlExecutionLogger.info(f'Read Batches Count  : {source_read_batch}')
        self.SqlExecutionLogger.info(f'Write Batches Count  : {target_write_batch}')
        self.SqlExecutionLogger.info(f"Full Load Target Execute SQL : {''.join(full_target_insert_sql)}")
        self.SqlExecutionLogger.info(f"Increment Load Target Execute SQL : {''.join(increment_target_upsert_sql)}")
        self.SqlExecutionLogger.info(f'Insert into {table_name} starts at {dt.now()},with Perform Transform Target as {perform_transform_target}')

        insert_sql = full_target_insert_sql  if load_type == FULL else increment_target_upsert_sql


        
        rowswritten = self.__execute_dft_queries(
                                                process_csv_rearrage_column,
                                                insert_sql,
                                                source_read_batch,
                                                target_write_batch,
                                                tgt_connection_obj    
                                                )
        tgt_row_count = rowswritten[1]
        self.SqlExecutionLogger.info(f'Target Row count :{tgt_row_count}')
        self.SqlExecutionLogger.info(f'Insert with  {load_type} into {table_name} ends at {dt.now()}')

        self.ObjTableConfiguration.Update_Table_config_wLast_refresh_date()
        self.SqlExecutionLogger.info('Table Config Json has been updated with Last refresh date.')

        return tgt_row_count

    def __delete_task(  self,
                        tgt_connection_obj,
                        activity):
        

        self.SqlExecutionLogger.info( f'Steps under {activity} will be  Executed')
        delete_sql = self.ObjTableConfiguration.del_hard_delete_sql 
        for query in delete_sql:
            delete_query = query
        error_message = 'Error executing for activity :{} on query :{}, with'.format(activity, delete_query) + ' error_message : {} .'
        tgt_connection_obj.execute_sql(delete_query, error_message)
        self.SqlExecutionLogger.info(' Number of Deleted  records from ODS Table is 0 .')

    def __post_trigger_task(self, 
                            tgt_connection_obj,
                            load_type):

        FULL = CommonVariables.full_load_type
        INCREMENTAL = CommonVariables.inc_load_type

        create_indexes_on_ODS_full = self.ObjTableConfiguration.post_create_ind_target_full_sql
        create_indexes_on_ODS_inc = self.ObjTableConfiguration.post_create_ind_target_incremental_sql
        other_sql = self.ObjTableConfiguration.post_other_target_sql

        for activity in CommonVariables.post_trigger_steps:

            if (activity == CommonVariables.task_create_indexes_full  and load_type == FULL): 
                self.__execute_queries_log( activity, 
                                        tgt_connection_obj,
                                        create_indexes_on_ODS_full)

            elif (activity == CommonVariables.task_create_indexes_inc and load_type == INCREMENTAL):
                self.__execute_queries_log(activity,
                                        tgt_connection_obj,
                                        create_indexes_on_ODS_inc)

            elif activity == CommonVariables.task_other_target_sql:
                self.__execute_queries_log(activity,
                                        tgt_connection_obj,
                                        other_sql)


    def __execute_load_plan(self,
                            target_table_name,
                            load_type, 
                            src_connection_obj, 
                            tgt_connection_obj):


        delete_flag = self.ObjTableConfiguration.del_enable_flag
        output_result = dict()

        for task in CommonVariables.execution_steps:

            if task == CommonVariables.pre_trigger_task:
                self.SqlExecutionLogger.info( f'Steps under {task} will be  Executed')
                self.__pre_trigger_task(
                                        tgt_connection_obj,
                                        load_type)

            elif (task == CommonVariables.data_flow_task and self.ObjTableConfiguration.data_flow_task is not None):
                self.SqlExecutionLogger.info( f'Steps under {task} will be  Executed')
                tgt_row_count = self.__data_flow_task(  target_table_name,
                                                        src_connection_obj,
                                                        tgt_connection_obj,
                                                        load_type)

            elif (task == CommonVariables.delete_task and delete_flag == CommonVariables.delete_flag_true):
                self.SqlExecutionLogger.info(f'Steps under {task} will be  Executed')
                self.__delete_task( src_connection_obj,
                                    tgt_connection_obj,
                                    task)

            elif task == CommonVariables.post_trigger_task:
                self.SqlExecutionLogger.info(f'Steps under {task} will be  Executed')
                self.__post_trigger_task(   tgt_connection_obj,
                                            load_type)

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
    

    def load () :
        # starts executing tasks from packages
            try:
                result = self.__execute_load_plan( tgt_tbl_name, 
                                                    load_type,
                                                    src_connection_obj,
                                                    tgt_connection_obj)
            except Exception as exception:
                self.SqlExecutionLogger.error(f'Error has been raised : {exception}', exc_info=True)
                result = dict ()
                result[CommonVariables.load_type] = load_type
                result[CommonVariables.result_of_load] = CommonVariables.status_failed
                result[CommonVariables.error_reason] = exception
                result[CommonVariables.num_of_rows_processed] = 0

            result[CommonVariables.table_schema] = tgt_schema_name
            result[CommonVariables.load_start_time] = start_time
            result[CommonVariables.table_name] = tgt_tbl_name
            result[CommonVariables.load_end_time] = dt.now()
            return_variable[self.table_name] = result
            db_obj.release_connection(tgt_connection_obj)

    def perform_etl(    self, 
                        load_type, 
                        db_obj:DatabaseInitialiser, 
                        ):

        start_time = dt.now()
        self.SqlExecutionLogger.info(
            'Start_time of ETL for {} ,{}'.format(self.table_name, start_time))
        
        return_variable = dict()

        try:

            tgt_connection = self.ObjTableConfiguration.target_connection
            tgt_schema_name = self.ObjTableConfiguration.target_schema_name
            tgt_tbl_name = self.ObjTableConfiguration.target_table_name
            try:
                src_connection_obj = 'KAFKA'
                tgt_connection_obj = db_obj.init_db_connection(
                                                                tgt_connection, 
                                                                self.SqlExecutionLogger, 
                                                                self.SqlExecutionLoggerFile, 
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
              
        except Exception as exc:
            self.SqlExecutionLogger.error(
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
            
        self.SqlExecutionLogger.info(f'End_time of ETL for  {self.table_name} , {dt.now()}')

        return return_variable
