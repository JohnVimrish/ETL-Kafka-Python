from datetime import datetime as dt
from jsoncustom.configparameters import TableConfigParametersValue,ConfigParametersValue
from database.dbinitialisers import DatabaseInitialiser
from root.commonvariables import CommonVariables
from util.loggingutility import LoggingUtil
from util.stringutil import StringUtility
from datetime import datetime

class SQLExecution():

            def __init__(self,
                        table_name,
                        ObjLogging: LoggingUtil,
                        table_log_file ,                                   
                        config_filepath):
                self.table_name = table_name
                self.SqlExecutionLogger = ObjLogging.setup_logger(table_name, ConfigParametersValue.json_sql_value_extc_log_level)
                self.ObjLogging = ObjLogging
                self.SqlExecutionLoggerFile = table_log_file
                ObjLogging.link_logger_filehandler(self.SqlExecutionLogger, self.SqlExecutionLoggerFile)
                self.ObjTableConfiguration  = TableConfigParametersValue(config_filepath,self.ObjLogging,self.SqlExecutionLoggerFile)

            def __init_output_result (self ) :
                self.output_dict = dict()
                self.output_dict[CommonVariables.load_type] = self.load_type
                self.output_dict[CommonVariables.table_schema] =  self.ObjTableConfiguration.target_schema_name
                self.output_dict[CommonVariables.load_start_time] = self.load_start_time
                self.output_dict[CommonVariables.table_name] = self.ObjTableConfiguration.target_table_name


            def __data_type_conversion(self,convert_value, conversion_match ) :
                c_dtype = self.ObjTableConfiguration.dft_tgt_column_dtype
                if c_dtype[conversion_match]=='str':
                   return  str(convert_value)
                elif c_dtype[conversion_match]=='int' :
                    return   int(convert_value)
                elif c_dtype[conversion_match]=='float' :
                    return   float(convert_value)    
                elif c_dtype[conversion_match]=='datetime' :
                    return  datetime.utcfromtimestamp(convert_value)
            
            def _initialise_database_etl(    self, 
                                            load_type, 
                                            db_obj:DatabaseInitialiser, 
                                            ):

                self.load_start_time = dt.now()
                self.SqlExecutionLogger.info(
                    'Start_time of ETL for {} ,{}'.format(self.table_name, self.load_start_time))        
                tgt_connection = self.ObjTableConfiguration.target_connection
                self.load_type = load_type
                try:
                    self.src_connection_obj = 'KAFKA'
                    self.tgt_connection_obj = db_obj.init_db_connection(
                                                                    tgt_connection, 
                                                                    self.SqlExecutionLogger, 
                                                                    self.SqlExecutionLoggerFile, 
                                                                    self.ObjLogging,
                                                                    self.table_name)
                    self.__init_output_result()
                except:
                    error_dict = dict()
                    self.output_dict[CommonVariables.result_of_load] = CommonVariables.failed_load
                    error_dict[CommonVariables.error_reason] = 'Connection cannot be established as requested connection in config.json is missing from program\'s connection parameter.json'
                    error_dict[CommonVariables.load_end_time] = dt.now()
                    error_dict[CommonVariables.num_of_rows_processed] = 0
                return StringUtility(self.output_dict,error_dict)
            

            def __exception_raiser(self,error_status) :
                error_dict = dict()
                self.SqlExecutionLogger.error(
                    'Error has been raised : {} .'.format(error_status), exc_info=True)            
                error_dict[CommonVariables.result_of_load] = CommonVariables.failed_load
                error_dict[CommonVariables.error_reason] = error_status
                error_dict[CommonVariables.num_of_rows_processed] = 0
                return  error_dict

            def __execute_queries_log(  self, 
                                    activity, 
                                    sql_list):
                separator = CommonVariables.comma_seperator
                self.SqlExecutionLogger.info(f'Queries of  {activity} : {separator.join(sql_list)}')        
                self.__execute_queries( 
                                    activity, 
                                    sql_list)
                self.SqlExecutionLogger.info(f'Queries with  {activity} Has Executed')

            def __execute_queries(  self, 
                                    activity, 
                                    sql_list
                                    ):
                if len(sql_list) > 0 :
                    for sql in sql_list:
                        error_message = f'Error executing for activity :{activity} on query :{sql}, with' + ' error_message : {} '
                        self.tgt_connection_obj.execute_sql(sql, error_message)
                
            def __pre_trigger_task(self):

                FULL = CommonVariables.full_load_type
                INCREMENTAL = CommonVariables.inc_load_type

                other_sql = self.ObjTableConfiguration.pre_other_target_sql 
                trunc_ods_tbl_sql = self.ObjTableConfiguration.pre_trun_target_full_sql
                trunc_ods_tbl_inc_sql = self.ObjTableConfiguration.pre_trun_target_inc_sql

                for activity in CommonVariables.pre_trigger_steps:
                    
                    if activity == CommonVariables.task_other_sql:
                        self.__execute_queries_log(activity,other_sql)                

                    elif (activity == CommonVariables.task_truncate_tbl_full_sql and self.load_type == FULL):
                        self.__execute_queries_log(activity,trunc_ods_tbl_sql)

                    elif (activity == CommonVariables.task_truncate_tbl_inc_sql and self.load_type == INCREMENTAL):
                        self.__execute_queries_log(activity,trunc_ods_tbl_inc_sql)

            def __data_flow_task(self, 
                                insert_data):
                

                def execute_dft_queries(
                                    rearrange_csv_columns,
                                    input_data, 
                                    target_executable_sql):

                    for value in target_executable_sql:
                        execute_query = value
                    total_src_rows  = len(input_data)
                    self.SqlExecutionLogger.debug(f'column rearrange and conversion to tupple array starts at {dt.now()}')
                    input_data_rearranged = list()
                    for each_dict in input_data  :
                        rearranged_insert_tuple  = list()
                        for column_name in rearrange_csv_columns:
                            rearranged_insert_tuple.append(self.__data_type_conversion(each_dict[column_name],column_name))
                        input_data_rearranged.append(tuple(rearranged_insert_tuple))
                    
                    self.SqlExecutionLogger.debug( f'column rearrange and conversion to tupple array ends at {dt.now()}')
                    self.tgt_connection_obj.upsert_data_in_batches(input_data_rearranged, execute_query)
                    self.SqlExecutionLogger.debug( f'Current Batch records are successfully  uploaded to target table in DB {dt.now()}')
                    total_tgt_rows = self.tgt_connection_obj.get_full_write_row_count()

                    return total_src_rows, total_tgt_rows

                process_csv_rearrage_column = self.ObjTableConfiguration.dft_tgt_columns
                insert_sql = self.ObjTableConfiguration.dft_insert_sql
                self.SqlExecutionLogger.info(f'DFT Load Type  : {self.load_type}')
                self.SqlExecutionLogger.info(f"Target Execute SQL : {''.join(insert_sql)}")

                rowswritten = execute_dft_queries(
                                                        process_csv_rearrage_column,
                                                        insert_data,
                                                        insert_sql
                                                        )
                tgt_row_count = rowswritten[1]
                self.SqlExecutionLogger.info(f'Target Row count :{tgt_row_count}')
                self.SqlExecutionLogger.info(f'Insert with  {self.load_type} into {self.table_name} ends at {dt.now()}')

                return tgt_row_count

            def __delete_task(  self,
                                activity,
                                delete_records:list):
                

                self.SqlExecutionLogger.info( f'Steps under {activity} will be  Executed')
                delete_sql = self.ObjTableConfiguration.delete_sql 
                for query in delete_sql:
                    delete_query = query
                error_message = 'Error executing for activity :{} on query :{}, with'.format(activity, delete_query) + ' error_message : {} .'
                for  each_delete_rec in delete_records :
                    delete_records = list()
                    for column_name in self.ObjTableConfiguration.delete_use_columns :
                       delete_records.append(self.__data_type_conversion(each_delete_rec[column_name],column_name))
                    self.tgt_connection_obj.execute_sql(delete_query, error_message,tuple(delete_records))
                self.SqlExecutionLogger.info(' Number of Deleted  records from ODS Table is 0 .')

            def __post_trigger_task(self):

                other_sql = self.ObjTableConfiguration.post_other_target_sql
                self.__execute_queries_log(CommonVariables.task_other_target_sql,other_sql)

            def execute_pre_trigger_task(self) :                 
                self.SqlExecutionLogger.info( f'Steps under Pre Trigger task will be  Executed')
                try :
                    self.__pre_trigger_task()
                except Exception as Error_Content:
                     return StringUtility.merge_dict(self.output_dict,self.__exception_raiser(Error_Content))
                

            def execute_data_flow_task(self,insert_data:list) : 
                try :
                    self.SqlExecutionLogger.info( f'Steps under Data Flow task will be  Executed')
                    if self.tgt_connection_obj.cursor_status()   : 
                        self.output_dict[CommonVariables.num_of_rows_processed] = self.__data_flow_task(insert_data)
                    else :
                        self.SqlExecutionLogger.info( f'cannot perform Data Flow task as database cursor as closed in  Pre Trigger task .') 
                except Exception as Error_Content:
                     return StringUtility.merge_dict(self.output_dict,self.__exception_raiser(Error_Content))

            def execute_delete_task(self, delete_records:list) : 
                try :
                    self.SqlExecutionLogger.info( f'Steps under Delete task will be  Executed')
                    if self.tgt_connection_obj.cursor_status()   : 
                        self.__delete_task(CommonVariables.delete_activity,delete_records)
                    else :
                        self.SqlExecutionLogger.info( f'cannot perform Delete task as database cursor as closed in  Post Trigger task .') 
                        self.SqlExecutionLogger.info( f'delete_records:{delete_records}')
                except Exception as Error_Content:
                     return StringUtility.merge_dict(self.output_dict,self.__exception_raiser(Error_Content))
        
            def execute_post_trigger_task(self) : 
                try :
                        self.SqlExecutionLogger.info( f'Steps under Post Trigger task will be  Executed')
                        if self.tgt_connection_obj.cursor_status()   : 
                            self.__post_trigger_task()
                        else :
                            self.SqlExecutionLogger.info( f'cannot perform Post Trigger Task  as database cursor as closed in  Data Flow task .') 
                except Exception as Error_Content:
                     return StringUtility.merge_dict(self.output_dict,self.__exception_raiser(Error_Content))
                
            def execute_dft_delete_task(self, insert_data,delete_data) :
                try :
                    if self.execute_data_flow_task(insert_data) is  None :
                        return self.execute_delete_task(delete_data)
                except Exception as Error_Content:
                     return StringUtility.merge_dict(self.output_dict,self.__exception_raiser(Error_Content))

                
            def release_acquired_db_connection (self) :
                self.tgt_connection_obj.release_connection()

            def load_plan_status(self):
                output_result = dict() 
                if self.tgt_connection_obj.cursor_status()   :           
                    output_result[CommonVariables.result_of_load] = CommonVariables.failed_load
                    output_result[CommonVariables.error_reason] = 'Cursor closed and not allowing other queries to be executed.'
                else:
                    output_result[CommonVariables.load_type] = self.load_type
                    output_result[CommonVariables.result_of_load] = CommonVariables.successfull_load
                self.release_acquired_db_connection ()
                self.SqlExecutionLogger.info(f'End_time of ETL for  {self.table_name} , {dt.now()}')
                return StringUtility.merge_dict(self.output_dict,output_result)

