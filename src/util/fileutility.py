# importing the os module
from os.path import exists, isfile, join
from os import makedirs, listdir
import sys
import json
import re



# to get the current working directory

class FileUtility():
    """_summary_

    Returns:
        _type_: _description_
    """    

    @staticmethod
    def recreatefile(filepath, logger=''):
        """_summary_

        Args:
            filepath (_type_): _description_
            logger (str, optional): _description_. Defaults to ''.
        """        
        try:
            open(filepath, "w+")
        except Exception as error_message:
            if 'No such file or directory:' in str(error_message):
                try:
                    directory = filepath.rsplit('/', 1)[0]
                    FileUtility.create_directory(directory)
                    open(filepath, "w+")
                except Exception as error_message:
                    logger.error(error_message, exc_info=True)
            else:
                logger.error(error_message, exc_info=True)

    @staticmethod
    def read_json_into_dict(file_path, logger):
        """_summary_

        Args:
            file_path (_type_): _description_
            logger (_type_): _description_

        Returns:
            _type_: _description_
        """        
        try:
            with open(file_path, "r") as jsfile:
                inputjson = json.load(jsfile)
            jsfile.close()
            return inputjson
        except Exception as error_message:
            logger.error(error_message, exc_info=True)

    @staticmethod
    def create_directory(folder_path, logger='print'):
        """_summary_

        Args:
            folder_path (_type_): _description_
            logger (str, optional): _description_. Defaults to 'print'.
        """        
        try:
            if not exists(folder_path):
                makedirs(folder_path)
        except OSError as error_message:
            if logger != 'print':
                logger.error(error_message, exc_info=True)
            else:
                print(error_message)

    @staticmethod
    def get_local_file_list(file_format,search_location) :
        """_summary_

        Args:
            file_format (_type_): _description_
            search_location (_type_): _description_

        Returns:
            _type_: _description_
        """        
        pattern_match ='.*{0}'.format(file_format)
        return  [ file.replace(file_format,'') for file in listdir(search_location) if
                        (isfile(join(search_location, file)) and re.search(pattern_match, file))]
    @staticmethod
    def write_to_json(file_path, input_dict):
        """_summary_

        Args:
            file_path (_type_): _description_
            input_dict (_type_): _description_
        """        
        with open(file_path, 'w') as f:
            json.dump(input_dict, f)

