from psycopg2 import pool as Postgres_Connect_pool, Error, DatabaseError as db_error, OperationalError
import sys
from jsoncustom.jsontagvariables import JsonTagVariables
from root.commonvariables import CommonVariables
from jsoncustom.configparameters import ConfigParametersValue
from util.loggingutility import LoggingUtil


class PostgresConnectionPool():

    def __init__(self,
                 db_connector_name,
                 config_connect_vars: dict,
                 log_obj: LoggingUtil,
                 logger_file_handler):
        """_summary_

        Args:
            db_connector_name (_type_): _description_
            config_connect_vars (dict): _description_
            log_obj (LoggingUtil): _description_
            logger_file_handler (_type_): _description_
        """        

   
        # Logger Initialization
        self.pg_cp_logger = log_obj.setup_logger(CommonVariables.postgres_con_pool, ConfigParametersValue.postgres_connector_log_level)
        log_obj.link_logger_filehandler(self.pg_cp_logger, logger_file_handler)

        # Config Values Initializatoin
        user_id = config_connect_vars[JsonTagVariables.user_id]
        password = config_connect_vars[JsonTagVariables.password]
        port = config_connect_vars[JsonTagVariables.port]
        host = config_connect_vars[JsonTagVariables.host]
        min_connections = config_connect_vars[JsonTagVariables.min_connection_pools]
        max_connections = config_connect_vars[JsonTagVariables.max_connection_pools]
        database = config_connect_vars[JsonTagVariables.database]
        db_type = config_connect_vars[JsonTagVariables.database_type]
        self.db_connector_name = db_connector_name

        try:
            self.connection_pool = Postgres_Connect_pool.ThreadedConnectionPool(
                minconn=min_connections,
                maxconn=max_connections,
                user=user_id,
                password=password,
                host=host,
                port=port,
                database=database)
            
            str1 = f'{db_connector_name} Connector with  user_name:{user_id}, password : xxxx, '
            str2 = f'host :{host}, port :{port},database : {database}, '
            str3 = f'min_num of connections :{min_connections},'
            str4 = f'max_num of connections :{max_connections}, db_type :{db_type}  has connected !'

            self.pg_cp_logger.info(str1 + str2 + str3 + str4 )

        except (OperationalError, Exception, Error) as err:
            # passing exception to function
            self.pg_cp_logger.critical('{0} Connector  has failed to connect with error :{1}'.format(
                db_connector_name, err), exc_info=True)

    def get_connection_pool(self):
        """_summary_

        Returns:
            _type_: _description_
        """        
        return self.connection_pool

    def log_pgdb_exception(self, err_msg_obj):
        """_summary_

        Args:
            err_msg_obj (_type_): _description_

        Returns:
            _type_: _description_
        """        
        # get details about the exception
        err_type, err_obj, traceback = sys.exc_info()
        # get the line number when exception occured
        line_n = traceback.tb_lineno
        # print the connect() error

        str1 = f' npsycopg2 ERROR:{err_msg_obj} on line number:{line_n}\npsycopg2 traceback:{traceback}'
        str2 = f' -- type:{err_type}\npgerror:{err_msg_obj.pgerror}\n pgcode: {err_msg_obj.pgcode}'
        return str1 + str2

    def log_error(self, log_statement, error_message):
        """_summary_

        Args:
            log_statement (_type_): _description_
            error_message (_type_): _description_
        """        
        self.pg_cp_logger.error(log_statement.format(
            self.log_pgdb_exception(error_message)), exc_info=True)

    def close_connection_pool(self, connection_name, log_message):
        """_summary_

        Args:
            connection_name (_type_): _description_
            log_message (_type_): _description_
        """        
        try  :
            try:
                self.connection_pool.closeall()
                self.pg_cp_logger.info(
                    log_message.format(connection_name))
            except (Error, db_error) as error:
                self.log_error(
                    'Unable to Close Connection pool , Error :{} .', error)
        except :
            self.pg_cp_logger.info('Unable to Close Connection pool as Postgres Connection pool is not initialised ')
        

