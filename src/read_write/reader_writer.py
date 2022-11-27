from typing import Type

from pyspark.sql import DataFrame

from src.computations.schema import DataModel
from src.utils.job_context import JobContext


class ReaderWriter:
    @classmethod
    def _read_parquet_file(cls, jc: JobContext, file_name: str) -> DataFrame:
        return jc.spark.read.parquet(file_name)

    @classmethod
    def read_table(cls, jc: JobContext, data_model: Type[DataModel]) -> DataFrame:
        file_name = data_model.data_path()
        return cls._read_parquet_file(jc=jc, file_name=file_name)


