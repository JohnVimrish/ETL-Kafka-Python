from  root.commonvariables import CommonVariables

class StringUtility () :

    @staticmethod
    def derive_actual_directory (to_concat_path) :
        """_summary_

        Args:
            to_concat_path (_type_): _description_

        Returns:
            _type_: _description_
        """        
        return CommonVariables.etl_project_directory + to_concat_path

    @staticmethod
    def derive_table_config_actual_directory (to_concat_path,json_file) :
        """_summary_

        Args:
            to_concat_path (_type_): _description_
            json_file (_type_): _description_

        Returns:
            _type_: _description_
        """        
        return CommonVariables.etl_project_directory  + to_concat_path + json_file

    @staticmethod
    # Python code to merge dict using update() method
    def merge_dict(dict1:dict, dict2:dict):
        """_summary_

        Args:
            dict1 (dict): _description_
            dict2 (dict): _description_

        Returns:
            _type_: _description_
        """        
        for keys,values in dict2.items() :
            dict1[keys] = values
        return dict1
 