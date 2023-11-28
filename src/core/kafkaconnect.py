import  confluent_kafka as kafka
import confluent_kafka.error as kafka_error
from  root.commonvariables import CommonVariables
from jsoncustom.configparameters import ConfigParametersValue
from database import dbinitialisers
import time


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

    def __process_messages_from_topics(self):
            
            message_status = CommonVariables.successfull_load
            try:
                # Poll for messages
                message = self.kafka_consumer_client.poll(timeout=10)  # Adjust timeout as needed based on waiting time needed to acquire message 
                if message is None:
                    # No messages received, continue polling
                    self.KafkaETLProcess.info ('Waiting No message produced yet !.')
                else:
                    if message.error():
                        # Handle any errors
                        self.KafkaETLProcess.error(f"Error in Produced Message : {message.error()}")
                        message_status = CommonVariables.failed_load
                    else :
                        # Process the message
                        self.KafkaETLProcess.info(f" Produced Message :key-{message.key()}, Message-{message.value()}")
                        # Commit the offsets if necessary
                        self.kafka_consumer_client.commit()
            except kafka_error.KafkaError as error_message:
                    self.KafkaETLProcess.error('Error  in Consumer Pipeline : ',error_message)
                    message_status = CommonVariables.failed_load
            return message_status
       

    
    def __partition_metadata(self, topic_name):
       partition_metadata = [kafka.TopicPartition(topic_name,int(partition.__dict__['id'])) for partition in self.topic_metadata.get(topic_name)]

    def consume_messages(self, topic,batch_interval_seconds,load_type,DatabaseObj:dbinitialisers):
        start_time = time.time()
        self.load_type = load_type 
        if self.ConsumerLoadStatus != CommonVariables.failed_load :
            try :
                self.__subscribe_topic(topic)
                status_monitor = list()
                while True:
                    ConsumerLoadStatus= self.__process_messages_from_topics()
                    status_monitor.append(ConsumerLoadStatus)
                    # Check if batch interval has elapsed
                    if time.time() - start_time >= batch_interval_seconds:
                        break
                    elif  CommonVariables.failed_load in status_monitor :
                        self.ConsumerLoadStatus = CommonVariables.failed_load 
                        break 
                    else :
                         self.ConsumerLoadStatus = CommonVariables.successfull_load 
                         continue 
                
            except Exception as error  :
                    self.KafkaETLProcess.error('Error  in Consumer Pipeline : ',error)
                    self.ConsumerLoadStatus = CommonVariables.failed_load 
        self.KafkaETLProcess.info('Kafka ETL Process , has Ended !')             
        return self.ConsumerLoadStatus

    def close_consumer(self):
        self.kafka_consumer_client.close()
