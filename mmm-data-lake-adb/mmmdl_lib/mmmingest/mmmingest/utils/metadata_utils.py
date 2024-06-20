from pyspark.sql import functions as F


class MetadataUtils:

    def __init__(self, spark, env, **kwargs):
        self.__dict__ = {**{'spark': spark, 'env': env}, **kwargs}

    def get_table_config(self):
        if self.source_system_name == "mmmplan" or "controller":
            table_config = self.spark.sql(f"select * from mmm_metadate.mmm_tables").where(
                (F.col("source_system_name") == self.source_system_name) & (
                        F.col("source_object_type") == self.source_object_type) & (
                        F.col("source_path_name") == self.source_path_name) & (
                        F.col("source_object_name") == self.source_object_name)).collect()
            if not table_config:
                raise Exception("No entry found in the metadate config table for the processed object")
            return table_config[0]
        else:
            table_congig = self.spark.sql(f"select * from mmm_metadate.mmm_tables").where(
                (F.col("source_system_name") == self.source_system_name) & (
                        F.col("source_object_type") == self.source_object_type) & (
                        F.col("source_path_name") == self.source_path_name) & (
                        F.col("source_schema_name") == self.source_schema_name) & (
                        F.col("source_object_name") == self.source_object_name)).collect()
            
    def get_column_config(self):
        if self
            




