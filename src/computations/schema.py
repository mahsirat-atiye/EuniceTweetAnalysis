from pyspark.sql.types import StructField
from pyspark.sql.types import (
    StringType,
    TimestampType,
    ArrayType,
    IntegerType,
)

import abc

from src.utils.paths import project_source_root
import logging

logger = logging.getLogger(__name__)


class DataModel(metaclass=abc.ABCMeta):
    @classmethod
    @abc.abstractmethod
    def data_path(cls) -> str:
        pass


class Tweets(DataModel):
    project = StructField(name='project', dataType=StringType(), nullable=False)
    id = StructField(name='id', dataType=StringType(), nullable=False)
    author_id = StructField(name='author_id', dataType=StringType(), nullable=False)
    created_at = StructField(name='created_at', dataType=TimestampType(), nullable=False)
    year = StructField(name='year', dataType=TimestampType(), nullable=False)
    month = StructField(name='month', dataType=TimestampType(), nullable=False)
    day = StructField(name='day', dataType=TimestampType(), nullable=False)

    text = StructField(name='text', dataType=StringType())
    source = StructField(name='source', dataType=StringType())
    lang = StructField(name='lang', dataType=StringType())
    currency_symbol = StructField(name='currency_symbol', dataType=StringType())
    entities = StructField(name='entities', dataType=StringType())
    public_metrics = StructField(name='public_metrics', dataType=StringType())
    edit_history_tweet_ids = StructField(name='edit_history_tweet_ids', dataType=ArrayType(StringType()))

    @classmethod
    def data_path(cls) -> str:
        return str(project_source_root()) + '/data/tweets-archive'


class ProjectMetrics(DataModel):
    project = StructField(name='project', dataType=StringType(), nullable=False)
    time = StructField(name='time', dataType=TimestampType(), nullable=False)
    tweets_count = StructField(name='tweets_count', dataType=IntegerType())
    unique_tweets_count = StructField(name='unique_tweets_count', dataType=IntegerType())

    @classmethod
    def data_path(cls) -> str:
        return str(project_source_root()) + '/data/metrics'
