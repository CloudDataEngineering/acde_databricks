class DatabricksUtils:

    @staticmethod
    def get_environment(dbutils):
        scope_str, env = ' '.join([_.name for _ in dbutils.security.listScopes()]).lower(), ''
        dev, sit, uat, prd = 'dev', 'sit', 'uat', 'prd'
        if dev in scope_str:
            env = dev
        elif sit in scope_str:
            env = sit
        elif uat in scope_str:
            env = uat
        elif prd in scope_str:
            env = prd
        else:
            raise Exception(f'Unable to fetch Environment from scope  str: {scope_str}')
        return env
    
    @staticmethod
    def get_widget_params(dbutils, widget_keys):
        params = {}
        for widget_key in widget_keys:
            try:
                params[widget_key] = dbutils.widgets.get(widget_key)
            except:
                params[widget_key] = None
                return params
    
    @staticmethod
    def create_metastore_table(spark, db_name, table_name, table_path, metastore_name = 'hive_metastore'):
        spark.sql(f'CREATE DATABASE IF NOT EXISTS {metastore_name}.{db_name}')
         spark.sql(f'drop table if exists {metastore_name}.{db_name}.{table_name}')
         spark.sql(f'create table if not exists {metastore_name}.{db_name}.{table_name} using delta location "{table_path}"')

