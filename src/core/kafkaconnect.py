import confluent_kafka as kafka
import confluent_kafka.error as kafka_error
from root.commonvariables import CommonVariables
from jsoncustom.configparameters import ConfigParametersValue
from database import dbinitialisers
from core.sqlexecution import SQLExecution
import time
import re
from datetime import datetime as dt


class Kafka_CDC_Consumer():

    def __init__(self, consumer_config: dict,ObjLogger,LogFile,exclude_topics: list = [], topic_md :dict = None ):

        self.KafkaETLProcess    = ObjLogger.setup_logger(CommonVariables.kafka_connect_config,ConfigParametersValue.kafka_connect_log_level)
        ObjLogger.link_logger_filehandler(self.KafkaETLProcess,LogFile)
        self.KafkaETLProcess.info('Kafka ETL Process , has Started !')
        self.ObjLogger       = ObjLogger
        self.LogFile         = LogFile
        self.exclude_topics = exclude_topics
        self.topic_metadata = topic_md
        try:
            self.kafka_consumer_client = kafka.Consumer(consumer_config)
            self.ConsumerLoadStatus = CommonVariables.successfull_load
        except kafka_error.KafkaError as error_message:
           self.KafkaETLProcess.error('Error while creating consumer connection with Kafka :', error_message)
           self.ConsumerLoadStatus = CommonVariables.failed_load
        
    def __subscribe_to_all_topics(self):
        topics_to_subscribe = self.__init_kafka_topics()[1]
        self.kafka_consumer_client.subscribe(topics_to_subscribe)

    def __init_kafka_topics(self):
        consumer_topics  = list()
        topic_metadata   = dict ()
        for each_topic, topic_partition in self.kafka_consumer_client.list_topics().__dict__['topics'].items() :
               consumer_topics.append(each_topic)
               topic_metadata[each_topic] = [partition_metadata for partition_metadata in  topic_partition.partitions.values()]
        return topic_metadata,list(set(consumer_topics).difference(self.exclude_topics))
 
    def list_kafka_topics (self,) :
          return self.__init_kafka_topics()
        
    def __subscribe_topic(self, topic):
            
            def reset_offset(consumer, partitions):
                for partition in partitions:
                    # set kafka.OFFSET_END to each partition if incremental load to extract from recent message which are not committed and set kafka.OFFSET_BEGINNING if FULL load 
                    partition.offset = kafka.OFFSET_BEGINNING  if self.load_type == CommonVariables.full_load_type else kafka.OFFSET_END
                consumer.assign(partitions)
            try :
                  self.kafka_consumer_client.subscribe([topic],on_assign=reset_offset)
            except kafka_error.KafkaError as error_message:
                  self.KafkaETLProcess.error('Error  in Consumer Pipeline : ',error_message)

    def __process_messages_from_topics(self,batch_interval_seconds,insert_batc_interval:int, poll_timeout:int):
            
            def insert_data_to_var ( message,insert_var:list,delete_list:list) :
                message_operation = message['payload']['op']
                if message_operation in ['i','r','u']: 
                    insert_data.append(message['payload']['after'])
                elif message_operation in ['d']:
                     delete_list.append(message['payload']['before'])
                return insert_var,delete_list
            
            def process_insertion_of_data(insert_data:list,delete_data:list):
                if self.Database_Task_Executor.execute_dft_delete_task(insert_data,delete_data) is not None :
                    message_status = CommonVariables.failed_load 
                else:
                    # Commit the offsets if necessary
                    self.kafka_consumer_client.commit()
                    message_status = CommonVariables.successfull_load
                return message_status
                
            start_time  = dt.now()
            waiting_time  = dt.now()
            waiting_counter = 0 
            insert_data = list()
            delete_data = list()
            while True:
                    # Check if batch interval has elapsed
                    if time.time() - start_time.timestamp() >= batch_interval_seconds:
                        break
                    else :
                        try:
                            # Poll for messages
                            message = self.kafka_consumer_client.poll(timeout=poll_timeout)  # Adjust timeout as needed based on waiting time needed to acquire message 
                            if message is None :
                                if time.time() - waiting_time.timestamp() >= batch_interval_seconds:
                                    message_status = process_insertion_of_data(insert_data,delete_data)
                                    break
                                if waiting_counter == 0 :
                                     waiting_time  = dt.now()
                                # No messages received, continue polling
                                self.KafkaETLProcess.info ('Waiting No message produced yet !.')
                            else:
                                if message.error():
                                    # Handle any errors
                                    self.KafkaETLProcess.error(f"Error in Produced Message : {message.error()}")
                                    message_status = CommonVariables.failed_load
                                    break 
                                else :
                                    insert_data,delete_data = insert_data_to_var(message.value(),insert_data,delete_data)
                                    if insert_batc_interval == len(insert_data):                                         
                                       message_status  = process_insertion_of_data(insert_data,delete_data)
                                       if message_status == CommonVariables.successfull_load :
                                            waiting_counter =  0 
                                       else :
                                            break 
                        except (kafka_error.KafkaError,Exception) as error_message:
                                self.KafkaETLProcess.error('Error  in Consumer Pipeline : ',error_message)
                                message_status = CommonVariables.failed_load
                        continue 
            return message_status

    def __partition_metadata(self, topic_name):
       partition_metadata = [kafka.TopicPartition(topic_name,int(partition.__dict__['id'])) for partition in self.topic_metadata.get(topic_name)]

    def consume_messages(self, topic,batch_interval_seconds,load_type,DatabaseObj:dbinitialisers):
        
        def assign_load_failed () :
            self.ConsumerLoadStatus = CommonVariables.failed_load  
            self.Database_Task_Executor.release_acquired_db_connection ()
        
        self.load_type = load_type 
        if self.ConsumerLoadStatus != CommonVariables.failed_load :
            try :
                self.__subscribe_topic(topic)
                # Use regex to find the second occurrence of '.'
                match = re.search(r'(?<=\..*\.)(\w+)', topic)
                if match:
                    table_name = match.group(1)
                    self.Database_Task_Executor = SQLExecution(table_name,
                            self.ObjLogger,
                            self.LogFile,                                   
                            ConfigParametersValue.table_configuration_file_directory+'{table_name}.json')
                    if self.Database_Task_Executor._initialise_database_etl(self.load_type,DatabaseObj) is None :
                        if self.Database_Task_Executor.execute_pre_trigger_task() is None :
                            if  self.__process_messages_from_topics(batch_interval_seconds,
                                    ConfigParametersValue.message_wait_exaust_interval_ins,ConfigParametersValue.kafka_message_poll_wait_inms) == CommonVariables.successfull_load :
                                if self.Database_Task_Executor.execute_post_trigger_task()  is None :
                                    self.ConsumerLoadStatus = self.Database_Task_Executor.load_plan_status()[CommonVariables.result_of_load]
                                else:
                                    assign_load_failed ()
                            else:
                                assign_load_failed ()
                        else:
                            assign_load_failed ()
                    else:
                        self.KafkaETLProcess.error('Error  in  Database Connection ,check configuration file .')
                        self.ConsumerLoadStatus = CommonVariables.failed_load  
                else:  
                    self.KafkaETLProcess.info ('{match}, the output we got while performing regex.')
                    self.KafkaETLProcess.error('Error   because not able to extract table name to process ETL .')  
                    self.ConsumerLoadStatus = CommonVariables.failed_load            
            except Exception as error  :
                    self.KafkaETLProcess.error('Error  in Consumer Pipeline : ',error)
                    self.ConsumerLoadStatus = CommonVariables.failed_load 
        self.KafkaETLProcess.info('Kafka ETL Process , has Ended !')             
        return self.ConsumerLoadStatus

    def close_consumer(self):
        self.kafka_consumer_client.close()
