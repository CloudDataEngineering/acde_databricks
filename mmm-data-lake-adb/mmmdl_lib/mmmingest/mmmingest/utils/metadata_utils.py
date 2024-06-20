from pyspark.sql import functions as F


class MetadataUtils:

    def __init__(self, spark, env, **kwargs):
        self.__dict__ = {**{'spark': spark, 'env': env}, **kwargs}

    def get_table_config(self):
        if self.source_system_name == "mmmplan" or "controller":
            table_config = self.spark.sql(f"select * from mmm_metadate.mmm_tables").where(