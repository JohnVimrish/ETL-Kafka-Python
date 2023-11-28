from  root.commonvariables import CommonVariables
from jsoncustom.configparameters import ConfigParametersValue
from core.kafkaconnect import Kafka_CDC_Consumer
from database import dbinitialisers
from core.threadexecution import ThreadExecution



class Kafka_ETL_Process () :

    def __init__(self,ObjMainLogger,MainLogFile,load_type):

        self.KafkaETLProcess    = ObjMainLogger.setup_logger(CommonVariables.Kakfa_etl_config,ConfigParametersValue.kakfa_etl_log_level)
        ObjMainLogger.link_logger_filehandler(self.KafkaETLProcess,MainLogFile)
        self.KafkaETLProcess.info('Kafka ETL Process , has Started !')
        self.ObjMainLogger       = ObjMainLogger
        self.MainLogFile         = MainLogFile
        self.ETLProcessStatus    = CommonVariables.successfull_load
        self.LoadType            = load_type
        self.__init_default_kafka_consumer_configuration()

    def __init_kafka_Admin_client (self,configuration,exclude_topics) :
        try :
              return Kafka_CDC_Consumer (configuration,self.ObjMainLogger,self.MainLogFile,exclude_topics)
        except Exception as error  :
             self.KafkaETLProcess.error('Failed to Initialise Kafka Admin Client !')
             self.KafkaETLProcess.info(error)
             self.ETLProcessStatus = CommonVariables.failed_load
                
    def __init_default_kafka_consumer_configuration (self):
            self.configuration =  {
            'bootstrap.servers':ConfigParametersValue.kafka_bootstrap_servers,
            'group.id': ConfigParametersValue.default_consumer_group_id,
            'enable.auto.commit':ConfigParametersValue.enable_auto_commit , 
            'auto.offset.reset':ConfigParametersValue.default_offset_reset , # Disable auto-commit to manually manage offsets
            'max.poll.interval.ms':ConfigParametersValue.max_poll_interval_inms# Example: 5 minutes (adjust as needed)
            }

    def Process_ETL(self,DatabaseObj:dbinitialisers) :
            
        self.master_consumer = self.__init_kafka_Admin_client(self.configuration,ConfigParametersValue.exclude_topics)
        if self.ETLProcessStatus    == CommonVariables.successfull_load :
            topics_metadata,topics_list  = self.master_consumer.list_kafka_topics()
            self.KafkaETLProcess.info(f'List of Topics available for ETL Process  - {(',').join(topics_list)} .')
            ThreadExecutorObj =  ThreadExecution(self.ObjMainLogger,self.MainLogFile)
            ThreadExecutorObj._init_kafka_consumers(topics_list,topics_metadata,self.configuration, self.LoadType ) 
            self.ETLProcessStatus = ThreadExecutorObj.process_etl(ConfigParametersValue.max_threads_count_process,DatabaseObj,self.LoadType)
            self.master_consumer.close_consumer()    
             
        return self.ETLProcessStatus

