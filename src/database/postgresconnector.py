from psycopg2 import extras as ex, Error, DatabaseError as db_error
import sys
from datetime import datetime as dt
from util.loggingutility import LoggingUtil
from database.postgresconnectionpool import PostgresConnectionPool
from psycopg2.extensions import register_adapter, AsIs

class PostgresConnection():

    def __init__(self,
                 connection_pool: PostgresConnectionPool,
                 log_file_handler,
                 log_obj: LoggingUtil,
                 log_level,
                 table_name,
                 db_type):
        self.connection_pool = connection_pool
        logger_name = db_type + '-' + table_name
        self.pg_logger = log_obj.setup_logger(logger_name, log_level)
        log_obj.link_logger_filehandler(self.pg_logger, log_file_handler)
        register_adapter(np.int64,AsIs)
        self.write_row_count = 0

    def acquire_connection(self):
        try:
            self.new_connection = self.connection_pool.getconn()
            self.new_connection.autocommit = True
            self.cursor_object = self.new_connection.cursor()

        except db_error as error_message:
            self.pg_logger.critical('Database has failed to create a new connection  :{1}'.format(
                self.db_connector_name, error_message), exc_info=True)

    def log_and_close(self, log_statement, error_message):
        critical_error = ['server closed the connection unexpectedly']
        pg_exception_result = self.log_pgdb_exception(error_message)
        pg_error_msg   =pg_exception_result [1]
        if pg_error_msg in critical_error :
            self.pg_logger.critical(pg_exception_result [0], exc_info=True)
        self.pg_logger.error(log_statement.format(
           pg_exception_result [0]), exc_info=True)
        self.cursor_object.close()
    
    def __remove_ascii_zero_char (rows) :
        new_rows      = list()
        for row in  rows :
            new_row= [ col.replace(chr(0),'') if type(col) is str else col for col in row]
            new_rows.append(tuple(new_row))
        return new_rows

    def __upsert_data(self, 
                    write_inputs,
                    write_query):

        self.pg_logger.debug(f'Upsert data  write starts at {dt.now()}')

        ex.execute_values(  self.cursor_object, 
                            write_query,
                            write_inputs,
                            template=None,
                            page_size=5,
                            fetch=False)
        written_records_count = self.get_curr_exec_rowcount()
        self.write_row_count += written_records_count
        
        self.pg_logger.info(f'Number of Rows written :{written_records_count}')

    def upsert_data_in_batches(self,
                                input_data,
                                write_query):
        try:
            try:
                self.__upsert_data(input_data,
                                    write_query)
              
            except (Exception, db_error) as err:
                    if str(err) == 'A string literal cannot contain NUL (0x00) characters.':
                        try:
                            fixed_data = self.__remove_ascii_zero_char(input_data)
                            self.__upsert_data(   fixed_data,
                                                write_query)
                        except (Exception, db_error) as error:
                            self.log_and_close(
                                'Error  has occured while  executing the source results on  Target DB {}.', err)
                    else:
                        self.log_and_close(
                            'Error  has occured while  executing the source results on  Target DB {}.', err)
        except (Exception) as error:
            self.pg_logger.error(
                'Error occured in Method Execute_batch_results :{} .'.format(error), exc_info=True)

    def execute_sql(self, sql, error_message,query_argments = None):
        try:
            if self.new_connection == None:
                self.acquire_connection_Ncursor()
            self.cursor_object.execute(sql,vars=query_argments)
        except (Error, db_error) as error:
            self.log_and_close(error_message, error)
    
    def get_full_write_records(self) :
        return self.cursor_object.fetchall()

    def get_full_write_row_count(self):
        return self.write_row_count
    
    def get_full_records_asdict_list (self) :
        execution_query_description = self.cursor_object.description
        column_names = [column_name[0] for column_name in execution_query_description]
        return [dict(zip(column_names,each_row))   for each_row in self.get_full_write_records()]

    def get_curr_exec_rowcount(self):
        return self.cursor_object.rowcount

    def cursor_status(self):
        return self.cursor_object.closed

    def release_connection(self):
        try:
            self.connection_pool.putconn(self.new_connection, close=True)
        except (Error, db_error) as error:
            self.log_and_close(
                'Unable to Close Connection , Error :{} .', error)

    def log_pgdb_exception(self, err_msg_obj):
        # get details about the exception
        err_type, err_obj, traceback = sys.exc_info()
        # get the line number when exception occured
        line_n = traceback.tb_lineno
        # print the connect() error

        str1 = f' npsycopg2 ERROR:{err_msg_obj} on line number:{line_n}\npsycopg2 traceback:{traceback}'
        str2 = f' -- type:{err_type}\npgerror:{err_msg_obj.pgerror}\n pgcode: {err_msg_obj.pgcode}'
        return str1 + str2,err_msg_obj.pgerror
    


