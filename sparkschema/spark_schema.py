import importlib
import tempfile
from typing import Dict, List

from pyspark.sql import SparkSession, functions as sf, DataFrame, Column
from pyspark.sql.types import StructField, StructType, DecimalType, LongType, TimestampType, StringType, NullType

STRING_TYPE = StringType.__name__

LONG_TYPE = LongType.__name__

DECIMAL_TYPE = DecimalType.__name__

TIMESTAMP_TYPE = TimestampType.__name__

NULLTYPE = NullType.__name__

WHOLE_NR = r'[-+]?\d*'

DECIMAL = r'-?\d+(\.\d+)?'

TIMESTAMP = r'\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}\.\d{3}\+\d{4})?'


class SparkSchema:
    def __init__(self, sample_size: int = 10 * 1000):
        self.sample_size = sample_size

    def get_schema(self, df):
        df = self._write_to_parquet(df)
        col_types = self.determine_column_types(df)
        schema = self._rows_to_schema(df.columns, col_types)
        return schema

    def _rows_to_schema(self, columns: List[str], col_types: DataFrame):
        types = {r['col_name']: r for r in col_types.collect()}
        # use the original column order in the output fields so it works with CSV
        fields = [StructField(c, self._create_type(types.get(c))) for c in columns]
        schema = StructType(fields)
        return schema

    def _write_to_parquet(self, df: DataFrame) -> DataFrame:
        tmp = tempfile.mkdtemp()
        df.limit(self.sample_size).write.mode('overwrite').parquet(tmp)
        df = SparkSession.getActiveSession().read.parquet(tmp)
        return df

    def determine_column_types(self, df):
        df = self._determine_type_per_row(df)
        df = self._drop_values(df)
        df = self._count_types_per_column(df)
        df = self._select_most_occuring_type(df)
        return df

    def _select_most_occuring_type(self, df):
        df = df.groupBy('col_name').agg(sf.max_by('type', 'count').alias('type'),
                                        *self._first_decimals(df))
        return df

    def _count_types_per_column(self, df):
        stacking = self._stack(df)
        df = df.select(stacking, *self._get_dec_cols(df))
        df = df.filter(df['type'] != NULLTYPE)
        df = df.groupBy('col_name', 'type').agg(
            sf.count('*').alias('count'),
            *self._decimal_maxes(df)
        )
        return df

    def _stack(self, df):
        stacking = [f'"{c.replace("_type", "")}", {c}' for c in df.columns if not self._is_dec_col(c)]
        stacking = f'stack({len(stacking)}, {", ".join(stacking)}) as (col_name, type)'
        stacking = sf.expr(stacking)
        return stacking

    def _drop_values(self, df):
        df = df.select(*[c for c in df.columns if c.endswith('_type') or self._is_dec_col(c)])
        return df

    @staticmethod
    def _determine_type_per_row(df):
        # for each row determine the type of each column value
        for c in df.columns:
            type_col = f'{c}_type'
            df = df.withColumn(type_col,
                               sf.when(df[c].isNull(), NULLTYPE).otherwise(
                                   sf.when(df[c].rlike(TIMESTAMP), TIMESTAMP_TYPE).otherwise(
                                       sf.when(df[c].rlike(DECIMAL), DECIMAL_TYPE).otherwise(
                                           STRING_TYPE))))
            df = df.withColumn(f"{c}_whole_part",
                               sf.when(df[type_col] == DECIMAL_TYPE,
                                       sf.length(sf.regexp_extract(df[c], r'(^-?\d+)', 1))))
            df = df.withColumn(f"{c}_decimal_part",
                               sf.when(df[type_col] == DECIMAL_TYPE, sf.length(sf.split(df[c], r'\.').getItem(1))))
        return df

    @staticmethod
    def _create_type(r: Dict[str, any]):
        if not r:
            # no type could be found
            return StringType()
        else:
            col_type = r['type']
            module = importlib.import_module('pyspark.sql.types')
            my_class = getattr(module, col_type)
            col_name = r['col_name']
            if col_type == DecimalType.__name__:
                whole_part = r[f'{col_name}_whole_part']
                decimal_part = r[f'{col_name}_decimal_part']
                decimal_part = decimal_part if decimal_part else 0
                total = whole_part + decimal_part
                return my_class(total, decimal_part)
            else:
                return my_class()

    @staticmethod
    def _is_dec_col(c) -> bool:
        return c.endswith('_whole_part') or c.endswith('_decimal_part')

    def _get_dec_cols(self, df) -> List[str]:
        return [c for c in df.columns if self._is_dec_col(c)]

    def _decimal_maxes(self, df: DataFrame) -> List[Column]:
        return [sf.max(c).alias(c) for c in self._get_dec_cols(df)]

    def _first_decimals(self, df) -> List[Column]:
        return [sf.first(c).alias(c) for c in self._get_dec_cols(df)]
