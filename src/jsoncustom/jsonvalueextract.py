import json as js


class JsonValueExtractor():
    """_summary_
    """    

    def __init__(self,
                 file_location_path: str,
                 module_name = '',
                 logger_obj='print',
                 logger_Filehandler='',
                 log_level=0):
        """_summary_

        Args:
            file_location_path (str): _description_
            module_name (str, optional): _description_. Defaults to ''.
            logger_obj (str, optional): _description_. Defaults to 'print'.
            logger_Filehandler (str, optional): _description_. Defaults to ''.
            log_level (int, optional): _description_. Defaults to 0.
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
        """_summary_

        Args:
            logger_obj (_type_): _description_
            module_name (str): _description_
            logger_Filehandler (_type_): _description_
            log_level (_type_): _description_
        """        
        logger_name = f'{module_name}-json'
        self.json_logger = logger_obj.setup_logger(logger_name, log_level)
        logger_obj.link_logger_filehandler(self.json_logger, logger_Filehandler)
        self.print_error = False

    def json_val(self, json_key_path, seperator='--'):
        """_summary_

        Args:
            json_key_path (_type_): _description_
            seperator (str, optional): _description_. Defaults to '--'.

        Returns:
            _type_: _description_
        """        
        return self.json_value_extractor(json_key_path, seperator)

    def json_value_extractor(self, json_key_path, seperator):
        """_summary_

        Args:
            json_key_path (_type_): _description_
            seperator (_type_): _description_
        """        

        def json_extractor(parents_key_lst_from_root):
            """_summary_

            Args:
                parents_key_lst_from_root (_type_): _description_

            Returns:
                _type_: _description_
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
            """_summary_

            Returns:
                _type_: _description_
            """            
            json_keys_extracted = list()
            if seperator in json_key_path:
                json_keys_extracted = json_key_path.split(seperator)
            else:
                json_keys_extracted.append(json_key_path)

            return json_extractor(json_keys_extracted)
        except Exception as excep:
            """_summary_
            """
            if self.print_error is True:
                print('Error :', excep)
            else:
                self.json_logger.error('Error : {} .'.format(excep), exc_info=True)

