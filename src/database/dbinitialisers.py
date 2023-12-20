from logging import Logger
from jsoncustom.configparameters import ConfigParametersValue
from jsoncustom.jsontagvariables import JsonTagVariables
from root.commonvariables import CommonVariables
from util.loggingutility import LoggingUtil
from database.postgresconnectionpool import PostgresConnectionPool
from database.postgresconnector import PostgresConnection



class DatabaseInitialiser ():

    def __init__(self, 
                connectors_dict_list: list, 
                log_obj:LoggingUtil, 
                logger_filehandler):

        self.connections = list()
        self.connection_pools = dict()
        

        db_type = JsonTagVariables.database_type
        db_type_alias = JsonTagVariables.database_prod

        POSTGRESQL = CommonVariables.Postgres_Database

      
        self.db_init_logger = log_obj.setup_logger ( CommonVariables.db_init, ConfigParametersValue.db_initialiser_log_level )
        log_obj.link_logger_filehandler( self.db_init_logger, logger_filehandler)

        for connector_dict in connectors_dict_list:
            for db_connector, config_info in connector_dict.items():
                if config_info[JsonTagVariables.enable_connection] == 'Y':
                    db_connection_dict = dict()

                    if config_info[db_type] == POSTGRESQL:
                        connection_pool_obj = PostgresConnectionPool(   db_connector,
                                                                        config_info,
                                                                        log_obj,
                                                                        logger_filehandler)

                    db_connection_dict[JsonTagVariables.connection_pool] = connection_pool_obj
                    db_connection_dict[db_type_alias] = config_info[db_type]
                    self.connections.append(db_connector)
                    self.connection_pools[db_connector] = db_connection_dict
        self.db_init_logger.info(f"Overall Enabled Connections {','.join(self.connections)}")
                 
    def init_tgt_connections(self, 
                                tgt_connection_name:str,
                                thread_logger:Logger, 
                                thread_logger_file, 
                                log_obj:LoggingUtil, 
                                table_name:str):

        POSTGRESQL = CommonVariables.Postgres_Database
        postgres_log_level = ConfigParametersValue.postgres_connector_log_level
        if (tgt_connection_name in self.connections):

            for connection_name, connection_dict in self.connection_pools.items():
                
                connection_pool = connection_dict[JsonTagVariables.connection_pool].get_connection_pool()
                db_type = connection_dict[JsonTagVariables.database_prod]
                
                # Target DB Connection
                if ((tgt_connection_name == connection_name) and  db_type == POSTGRESQL):

                    try :
                        tgt_connection_obj = PostgresConnection( connection_pool,
                                                                thread_logger_file, 
                                                                log_obj, 
                                                                postgres_log_level, 
                                                                table_name, 
                                                                POSTGRESQL)
                        tgt_connection_obj.acquire_connection()
                        tgt_connection_obj.target_db_type = db_type
                        thread_logger.info(f'{tgt_connection_name} has connected as Target')
                    except Exception as error  :
                        thread_logger.error(error,exc_info=True)

            return  tgt_connection_obj
        else:
            thread_logger.info(f'Either Connection Name specified in Table Config Json : {tgt_connection_name} not found in Connection Config Json,or Connections are not properly intialised  by the program which are listed in Connection configuration file.')
            return None     

    def release_connection(self,db_connection_obj):
              db_connection_obj.release_connection()

    def close_connections(self):
        for connection_name, connection_dict in self.connection_pools.items():
            connection_dict[JsonTagVariables.connection_pool].close_connection_pool(connection_name, '{}  has been closed !')
