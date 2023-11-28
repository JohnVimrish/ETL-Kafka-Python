# importing the os module
from os.path import exists, isfile, join
from os import makedirs, listdir
import sys
import json
import re



# to get the current working directory

class FileUtility():

    @staticmethod
    def recreatefile(filepath, logger=''):
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
        try:
            with open(file_path, "r") as jsfile:
                inputjson = json.load(jsfile)
            jsfile.close()
            return inputjson
        except Exception as error_message:
            logger.error(error_message, exc_info=True)

    @staticmethod
    def create_directory(folder_path, logger='print'):
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
        pattern_match ='.*{0}'.format(file_format)
        return  [ file.replace(file_format,'') for file in listdir(search_location) if
                        (isfile(join(search_location, file)) and re.search(pattern_match, file))]
    @staticmethod
    def write_to_json(file_path, input_dict):
        with open(file_path, 'w') as f:
            json.dump(input_dict, f)

