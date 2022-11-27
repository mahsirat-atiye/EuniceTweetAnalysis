from typing import Dict, Optional

from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

spark_session = None


def get_spark() -> SparkSession:
    """
    get spark session, a global cache is kept
    """
    global spark_session
    if spark_session is None:
        spark_session = (
            SparkSession.builder.config('spark.sql.caseSensitive', 'true')
            .config("spark.sql.optimizer.maxIterations", 500)
            .appName("eunice")
            .getOrCreate()
        )

    return spark_session


class JobContext:
    job_contexts: Dict[Optional[str], "JobContext"] = {}

    @classmethod
    def get_context(cls, env: Optional[str] = None, spark: Optional[SparkSession] = None) -> "JobContext":
        if env in cls.job_contexts:
            return cls.job_contexts[env]

        if spark is None:
            spark = get_spark()

        jc = JobContext(spark=spark)

        cls.job_contexts[env] = jc

        return jc

    def __init__(self, spark: SparkSession):
        self.spark = spark
