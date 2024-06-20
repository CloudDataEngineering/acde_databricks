from delta.tables import DeltaTable


class DataLakeUtils:

    @staticmethod
    def read_from_lake_path(spark, lake_path, format = "delta"):
        if format == "csv":
            return spark.read.format(format).option("header", True).load(lake_path)
        else:
            return spark.read.format(format).load(lake_path)
    
    @staticmethod
    def write_to_lake_path(df, lake_path, format = "delta", mode = "append", overwriteSchema = False, 
                           partitionBycols = []):
        if overwriteSchema:
            df.write.format(format).mode(mode).option("overwriteSchema", True).partitionBy(partitionBycols).save(lake_path)
        else:
            df.write.format(format).mode(mode).option("mergeSchema", True).partitionBy(partitionBycols).save(lake_path)
    
    @staticmethod
    def update_delta_table(spark, lake_path, update_condition, update_value):
        delta_table = DeltaTable.forPath(spark, lake_path)
        delta_table.update(condition = update_condition, set = update_value)
    
    @staticmethod
    def is_delta_table(spark, lake_path):
        return DeltaTable.isDeltaTable(spark, lake_path)
    
    @staticmethod
    def merge_table(spark, source_df, target_table_path, condition = "source.mmm_hash = target.mmm_hash"):
        target_table = DeltaTable.forPath(spark, target_table_path)
        target_table.alias("target").merge(source_df.alias("source"), 
                                           condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    
    @staticmethod
    def lake_path_exists(dbutils, file_path):
        try:
            dbutils.fs.ls(file_path)
        except Exception as e:
            return False
        return True
