from concurrent.futures import ThreadPoolExecutor, as_completed
from database.dbinitialisers import DatabaseInitialiser
from core.kafkaconnect import Kafka_CDC_Consumer
from util.loggingutility import LoggingUtil
from root.commonvariables import CommonVariables
from jsoncustom.jsontagvariables import JsonTagVariables
from jsoncustom.configparameters import ConfigParametersValue
from util.fileutility import FileUtility

class ThreadExecution () :

    def __init__(self, 
                log_obj:LoggingUtil,
                log_file_handler
                ):

        # Setup Logger
        self.log_obj               = log_obj
        self.thread_exec_log_level = ConfigParametersValue.thread_exec_log_level
        self.thread_exec_logger = log_obj.setup_logger(CommonVariables.thread_exec,self.thread_exec_log_level)
        log_obj.link_logger_filehandler (self.thread_exec_logger,log_file_handler)
     
    def _init_kafka_consumers(self,topics:list,kafka_topic_md:dict,consumer_config : dict, load_type)  :
        self.process_topics_list    = dict()
        consumer_config[JsonTagVariables.auto_offset_reset] = CommonVariables.offset_reset_full if load_type == CommonVariables.full_load_type  else CommonVariables.offset_reset_inc
        for  topic in topics :
            log_folder =ConfigParametersValue.log_base_directory +'/'+CommonVariables.thread_log_folder +'/'
            TopicLoggerObj =  LoggingUtil(log_folder)
            topic_log_file= TopicLoggerObj.create_log_file(f'{topic.replace('.','')}.log')
            consumer_config[JsonTagVariables.group_id] = consumer_config.get(JsonTagVariables.group_id) + topic
            intialise_kafka_consumer_obj  = Kafka_CDC_Consumer(consumer_config,TopicLoggerObj,topic_log_file,topic_md= kafka_topic_md)
            self.process_topics_list[topic] = intialise_kafka_consumer_obj

    def process_etl(    self, 
                        max_threads_num,
                        DatabaseObj:DatabaseInitialiser,
                        load_type):
            
            table_etl_status = list ()
            FAILED= CommonVariables.failed_load
            SUCCESS=CommonVariables.successfull_load

            with ThreadPoolExecutor(max_workers = max_threads_num, 
                                    thread_name_prefix = CommonVariables.thread_prefix) as executor:

                #perform ETL for each table                 
                futures = [executor.submit(topic_obj.consume_messages,
                                                    topic,
                                                    ConfigParametersValue.consumer_live_status_interval_ins,
                                                    load_type,
                                                    DatabaseObj)
                                                    for topic,topic_obj  in self.process_topics_list.items()]
           
                # process each result as it is available
                for future_output in as_completed(futures):
                    try :
                        ## output of each table being processed
                        load_result= future_output.result()
                        table_etl_status.append(load_result)                            
                    except Exception as exc:
                        self.thread_exec_logger.error(f'Thread enabling exception: {exc}',exc_info=True)
                        table_etl_status.append(FAILED)
            return FAILED if FAILED in  table_etl_status  else SUCCESS

 