import json as js


class JsonValueExtractor():
    """  This class helps to extract values from simple/nested json  .
    """    

    def __init__(self,
                 file_location_path: str,
                 module_name = '',
                 logger_obj='print',
                 logger_Filehandler='',
                 log_level=0):
        """Intitialiser Method helping to create a specific instance based on individual files. 

        Args:
            file_location_path (str): json file location .
            module_name (str, optional): module name is defined as identify which logging mode it has to use. Defaults to ''.
            logger_obj (str, optional): Logger Object. Defaults to 'print'.
            logger_Filehandler (str, optional): Log File in which logging are stored . Defaults to ''.
            log_level (int, optional): Log level like (info or error or debug ). Defaults to 0.
        """        

        self.json_file_path = file_location_path
        if logger_Filehandler == '':
            self.print_error = True
        else:
            self.reinitialise_logger_object(logger_obj, module_name, logger_Filehandler, log_level)

        try:
            with open(self.json_file_path, "r") as confg:
                self.inputjson = js.load(confg)

        except Exception as excep:
            if self.print_error is True:
                print('Error :', excep)
            else:
                self.json_logger.error(f'Error : {excep} .', exc_info=True)

    def reinitialise_logger_object(self, logger_obj, module_name: str, logger_Filehandler, log_level):
        """ Re initialise the logger object  with json extract  class .
        Args:
            module_name (str, optional): module name is defined as identify which logging mode it has to use.
            logger_obj (str, optional): Logger Object. 
            logger_Filehandler (str, optional): Log File in which logging are stored .
            log_level (int, optional): Log level like (info or error or debug ).
        """        
      
        logger_name = f'{module_name}-json'
        self.json_logger = logger_obj.setup_logger(logger_name, log_level)
        logger_obj.link_logger_filehandler(self.json_logger, logger_Filehandler)
        self.print_error = False

    def json_val(self, json_key_path, seperator='--'):
        """ Json value extractor function , it tries to get vaues of json_key_path variable which are json  key tags that are concatnated using '--' as default for nested 
        json's.
        Args:
            json_key_path (_type_):  key pair  variable value in form of string , the path  to be mentioned can be direct or even nested.
            seperator (str, optional): seperator used for nested json key tag . Defaults to '--'.

        Returns:
            _type_: Values for specific keys provided as inputs 
        """        
        return self.json_value_extractor(json_key_path, seperator)

    def json_value_extractor(self, json_key_path, seperator):
        """Json value extractor function , it tries to get vaues of json_key_path variable which are json  key tags that are concatnated using '--' as default for nested 
        json's.

        Args:
            json_key_path (_type_):  key pair  variable value in form of string , the path  to be mentioned can be direct or even nested.
            seperator (_type_): seperator used for nested json key tag.
        """        

        def json_extractor(parents_key_lst_from_root):
            """
            Args:
                parents_key_lst_from_root (_type_): key pair  variable value in form of string , the path  to be mentioned can be direct or even nested.

            Returns:
                _type_: return value associated for the json key path  passed as input .
            """            
            key_lst = parents_key_lst_from_root
            curr_file = self.inputjson
            curr_json_dict = dict()
            total_parents = len(key_lst) - 1
            # max_len_of_path  = (parents_key_list-1)

            for parent_counter in range(total_parents + 1):

                current_parent = key_lst[parent_counter]
                current_val = curr_file.get(current_parent)

                if type(current_val) is dict:
                    if parent_counter != total_parents:
                        for next_parent_key, next_parent_dict in current_val.items():
                            curr_json_dict[next_parent_key] = next_parent_dict
                        curr_file = curr_json_dict
                    else:
                        return current_val
                else:
                    return current_val

        try:
       
            json_keys_extracted = list()
            if seperator in json_key_path:
                json_keys_extracted = json_key_path.split(seperator)
            else:
                json_keys_extracted.append(json_key_path)

            return json_extractor(json_keys_extracted)
        except Exception as excep:

            if self.print_error is True:
                print('Error :', excep)
            else:
                self.json_logger.error('Error : {} .'.format(excep), exc_info=True)

