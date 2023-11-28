from  root.commonvariables import CommonVariables

class StringUtility () :

    @staticmethod
    def derive_actual_directory (to_concat_path) :
        return CommonVariables.etl_project_directory + to_concat_path

    @staticmethod
    def derive_table_config_actual_directory (to_concat_path,json_file) :
        return CommonVariables.etl_project_directory  + to_concat_path + json_file
