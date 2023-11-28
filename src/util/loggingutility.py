import logging  as log 
from util.fileutility import FileUtility




class LoggingUtil () :

        def __init__(self,parent_director_path) :
             self.log_file_parentdirectory  = parent_director_path
             self.log_format                = '%(asctime)s:%(name)s:%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s'
             self.Pcommon_logger_level      = dict(CRITICAL=50,ERROR=40,WARNING=30,INFO=20,DEBUG=10,NOTSET=0)

        def setup_logger(self,logger_name,level):

                logger        = log.getLogger(logger_name)
                logger.setLevel(self.Pcommon_logger_level.get(level))
                return logger


        def create_log_file (self,log_file_name,specific_log_directory= '') :
                if specific_log_directory != '':
                        FileUtility.create_directory(self.log_file_parentdirectory+specific_log_directory)
                        log_file_path = self.log_file_parentdirectory +specific_log_directory+log_file_name
                else :
                        log_file_path = self.log_file_parentdirectory + log_file_name
                fileHandler   = log.FileHandler(log_file_path)
                formatter     = log.Formatter(self.log_format )
                fileHandler.setFormatter(formatter)     
                return  fileHandler           

        def link_logger_filehandler (self,logger,filehandler) :
                logger.addHandler(filehandler)    
                  
                        
        def __overall_summary_log_writer (self,summary_file_path :str ,summary_query :str) :
                db_error  = ' Overall Summary Logging Query error -'
                query = summary_query .format(self.ProgramProcessID)
                csv_input = self.__fetch_dictresults_Nlist (query,db_error)
                # CSVUtility.write_to_CSV(csv_input,summary_file_path)
