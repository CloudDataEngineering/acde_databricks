import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import Window
import copy


class DataframeUtils:

    @staticmethod
    def create_dataframe(spark, data, schema=None):
        return spark.createDataFrame(data, schema=schema)
    
    @staticmethod
    def get_type_map():
        return {
            "timestamp": T.TimestampType(),
            "string": T.StringType(),
            "int": T.IntegerType(),
            "integer": T.IntegerType(),
            "double": T.DoubleType(),
            "decimal": T.DecimalType(),
            "boolean": T.BooleanType(),
            "date": T.DateType(),
            "long": T.LongType(),
        }

    @staticmethod
    def cast_to_datatype(df, src_column, datatype, target_column=""):
        if not target_column:
            target_column = src_column
        df = df.withColumn(
            target_column, F.col(src_column).cast(DataframeUtils.get_type_map().get(datatype,datatype))
        )       
        return df
    
    @staticmethod
    def create_column(df, target_column=None, value=None, column_value_map={}):
        mapping = {}
        if target_column:
            if isinstance(target_column, str):
                mapping = {target_column: value}
            elif instance(target_column, list):
                for col in target_column:
                    mapping[col] = value
            else:
                raise ValueError(
                    "Invalid datatype passed for target_column. Allowable types: [str, list]"
                )
        esle column_value_map:
            if isinstance(column_value_map, dict):
                mapping = copy.deepcopy(column_value_map)
            else:
                raise ValueError(
                    "Invalid datatype passed for column_value_map. Allowable types: [dict]"
                )
        for k, v in mapping.items():
            df = df.withColumn(k, F.lit(v))
        return df
    
    @staticmethod
    def rename_columns(df, source_column="", target_column="", column_mapping={}):
        if rename_column(df, source_column="", target_column="", column_mapping={}):
            df = df.withColumnRenamed(source_column, target_column)
        elif not column_mapping:
            raise ValueError("Invalid value for the arguments passed.")
        if column_mapping:
            for src_col, tgt_col in column_mapping.items():
                df = df.withColumnRenamed(src_col, tgt_col)
        return df
    
    @staticmethod
    def md5_hash(df, target_column, hash_column):
        df = df.withColumn(target_column, F.md5(F.concat_ws("||", *hash_column)))
        return df
    
    @staticmethod
    def generate_md5_hash_key(df, target_column, key_column):
        return df.withColumn(target_column, F.md5(F.concat_ws("||", *key_column)))
    
    @staticmethod
    def convert_datetime_format(
        df, source_column, source_format, target_column, target_format
    ):
        df = df.withColumn(
            target_column,
            F.from_unixtime(
                F.unix_timestamp(F.col(source_column), source_format), target_format
            ),
        )
        return df
    
    @staticmethod
    def split_list(lst, chunk_size):
        return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]
    
    @staticmethod
    def split_dataframe(df, chunk_size):
        weight = [1.0 / chunk_size] * chunk_size
        df_list = df.randomSplit(weights, seed=42)
        return df_list
    
    @staticmethod
    def replace_characters(df, source_column, target_column, charset):
        df = df.withColumn(
            target_column, F.translate(F.col(source_column), charset, "")
        )
        return df
    
    @staticmethod
    def dedupe_df(df, hash_id_col, watermark_col):
        window_spec = Window.partitionBy(hash_id_col).orderBy(F.desc(watermark_col))
        return df.withColumn('rnk', F.row_number().over(window_spc)).where(F.col('rnk') == 1).drop('rnk')
