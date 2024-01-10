from  root.commonvariables import CommonVariables
from jsoncustom.configparameters import ConfigParametersValue
from core.kafkaconnect import Kafka_CDC_Consumer
from database import dbinitialisers
from core.threadexecution import ThreadExecution



class Kafka_ETL_Process () :

    def __init__(self,ObjMainLogger,MainLogFile,load_type):
        
        """Intialiser Method to Process kafka messages from Kafka Bootstrap Server.
        """
        self.KafkaETLProcess    = ObjMainLogger.setup_logger(CommonVariables.Kakfa_etl_config,ConfigParametersValue.kakfa_etl_log_level)
        ObjMainLogger.link_logger_filehandler(self.KafkaETLProcess,MainLogFile)
        self.KafkaETLProcess.info('Kafka ETL Process , has Started !')
        self.ObjMainLogger       = ObjMainLogger
        self.MainLogFile         = MainLogFile
        self.ETLProcessStatus    = CommonVariables.successfull_load
        self.LoadType            = load_type
        self.__init_default_kafka_consumer_configuration()

    def __init_kafka_Admin_client (self,configuration,exclude_topics) :
        """Initialising an Admin Kafka Consumer Client to Monitor Activities of Child Consumer , and fetch information all topic available within the 
        server and metadata informations regarding them.

        Args:
            configuration (_type_): Kafka Connection configuration
            exclude_topics (_type_): Exclude topics list to unprocess messages from .

        Returns:
            _type_: Returns Admin Kafka Consumer Object.
        """        
        try :
              return Kafka_CDC_Consumer (configuration,self.ObjMainLogger,self.MainLogFile,exclude_topics)
        except Exception as error  :
             self.KafkaETLProcess.error('Failed to Initialise Kafka Admin Client !')
             self.KafkaETLProcess.info(error)
             self.ETLProcessStatus = CommonVariables.failed_load
                
    def __init_default_kafka_consumer_configuration (self):
            """Setting up Kafka Connection  Configuration Variable 
            """            
            self.configuration =  {
            'bootstrap.servers':ConfigParametersValue.kafka_bootstrap_servers,
            'group.id': ConfigParametersValue.default_consumer_group_id,
            'enable.auto.commit':ConfigParametersValue.enable_auto_commit , 
            'auto.offset.reset':ConfigParametersValue.default_offset_reset , # Disable auto-commit to manually manage offsets
            'max.poll.interval.ms':ConfigParametersValue.max_poll_interval_inms# Example: 5 minutes (adjust as needed)
            }

    def Process_ETL(self,DatabaseObj:dbinitialisers) :
        """
        Intialiser Method to  Process Data from Kafka Server to Database.
        Args:
            DatabaseObj (dbinitialisers): Database Object to use for processing the messages 

        Returns:
            _type_: Returns Load Status on Processing Data from Kafka Server to Database 
        """        
            
        self.master_consumer = self.__init_kafka_Admin_client(self.configuration,ConfigParametersValue.exclude_topics)
        if self.ETLProcessStatus    == CommonVariables.successfull_load :
            topics_metadata,topics_list  = self.master_consumer.list_kafka_topics()
            self.KafkaETLProcess.info(f'List of Topics available for ETL Process  - {(',').join(topics_list)} .')
            ThreadExecutorObj =  ThreadExecution(self.ObjMainLogger,self.MainLogFile)
            ThreadExecutorObj._init_kafka_consumers(topics_list,topics_metadata,self.configuration, self.LoadType ) 
            self.ETLProcessStatus = ThreadExecutorObj.process_etl(ConfigParametersValue.max_threads_count_process,DatabaseObj,self.LoadType)
            self.master_consumer.close_consumer()    
             
        return self.ETLProcessStatus

