from datetime import datetime as dt
import sys
import traceback
from os.path import dirname as directoryname, realpath
from core.kafkaetlprocess import Kafka_ETL_Process
from database.dbinitialisers import DatabaseInitialiser
from jsoncustom.jsontagvariables import JsonTagVariables
from jsoncustom.jsonvalueextract import JsonValueExtractor
from jsoncustom.configparameters import ConfigParametersValue
from root.commonvariables import CommonVariables
from util.loggingutility import LoggingUtil
# from util.fusionemailsender import EMailSummaryLogAttachments

def main_function():
    """The main function executes all steps, starting with connecting to the Kafka Bootstrap Server using Python,
      utilizing configuration files that contain information on Kafka connection, ETL queries, log configurations, and database configuration.
      This process involves processing messages from all topics within the Kafka Bootstrap Server and storing them in the database.
    """
    config_json_object                             = JsonValueExtractor(sys.argv[1])
    connection_json_object                         = JsonValueExtractor(sys.argv[2])
    log_level_json_object                          = JsonValueExtractor(sys.argv[3])
    kafka_configuration_json_object                = JsonValueExtractor(sys.argv[4])

    ConfigParametersValue(config_json_object, connection_json_object,log_level_json_object,kafka_configuration_json_object)
                          
#     email_obj = EMailSummaryLogAttachments(ConfigParametersValue.smtp_server_host,
#                                            ConfigParametersValue.smtp_server_port,
#                                            ConfigParametersValue.email_sender_emailid,
#                                            ConfigParametersValue.email_receiver_list,
#                                            ConfigParametersValue.email_subjectline)

    try:
        MainLogOBJ = LoggingUtil(ConfigParametersValue.log_base_directory)
        KafkaETLMainLogger = MainLogOBJ.setup_logger(
            CommonVariables.main_config, ConfigParametersValue.kafka_main_log_level)
        CommonVariables.thread_log_folder = dt.now().strftime(ConfigParametersValue.main_log_file_name_format)
        main_logfile = MainLogOBJ.create_log_file(f'ETLProgram.log',f'{CommonVariables.thread_log_folder}/')
        MainLogOBJ.link_logger_filehandler(KafkaETLMainLogger,main_logfile)
        start_time = dt.now()
        KafkaETLMainLogger.info('ETL Start Time : {} .'.format(start_time))
        ObjJSONnCommonVariables = JsonTagVariables ()
        ObjJSONnCommonVariables.log_levels = ConfigParametersValue.all_packages_log_level
        KafkaETLMainLogger.info("Connection Pool Establishment starts!")
        ObjDBInitialiser  = DatabaseInitialiser(ConfigParametersValue.dbconnectors,
                                                            MainLogOBJ,
                                                            main_logfile)
        KafkaETLMainLogger.info("Connection Pool Establishment ends!")
        config_json_object.reinitialise_logger_object(
            MainLogOBJ, CommonVariables.main_config, main_logfile, ConfigParametersValue.json_value_extc_log_level)
        connection_json_object.reinitialise_logger_object(
            MainLogOBJ, CommonVariables.connection_param, main_logfile, ConfigParametersValue.json_value_extc_log_level)
        log_level_json_object.reinitialise_logger_object(
            MainLogOBJ, CommonVariables.log_level_config, main_logfile, ConfigParametersValue.json_value_extc_log_level)
        # email_obj.send_load_start_viamail(ConfigParametersValue.email_subjectline.format('has Started .'))
        ObjETLProcess = Kafka_ETL_Process(MainLogOBJ, main_logfile,ConfigParametersValue.load_type)
        ETLProcessStatus = ObjETLProcess.Process_ETL(ObjDBInitialiser)
        KafkaETLMainLogger.info('ETL Complete Status  : {} .'.format(ETLProcessStatus))
        KafkaETLMainLogger.info('ETL End Time : {} .'.format(dt.now()))
        ObjDBInitialiser.close_connections()
        
        # email_obj.send_load_status_viamail(DownloadProcessStatus,
        #                                     Overall_summary_file_path,
        #                                     Attachments,
        #                                     ConfigParametersValue.overall_summary_boc_columns_to_expose
        #                                     )
    except Exception as error:
        ex_type, ex_value, ex_traceback = sys.exc_info()
        # Extract unformatter stack traces as tuples
        trace_back = traceback.extract_tb(ex_traceback)
        # Format stacktrace
        stack_trace = list()
        for trace in trace_back:
           stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" %(trace[0], trace[1], trace[2], trace[3]))
           email_message = '\n'.join(["Exception type : %s " % ex_type.__name__,
                             "Exception message : %s" %ex_value ,
                             "Stack trace : %s" %stack_trace])
        #    email_obj.send_failed_load_status_viamail(
        #    ConfigParametersValue.email_subjectline.format(CommonVariables.status_failed), str(email_message))
           raise error

if __name__ == "__main__":
    CommonVariables.etl_project_directory = directoryname(directoryname(directoryname(realpath(__file__))))
    main_function()



 

