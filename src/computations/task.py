import abc
from pyspark.sql import DataFrame

from src.utils.job_context import JobContext
from src.utils.time_resource import clock
import logging

logger = logging.getLogger(__name__)

class DataTask(metaclass=abc.ABCMeta):
    def __init__(self):
        self.jc = JobContext.get_context()


    @abc.abstractmethod
    def read(self):
        pass

    @abc.abstractmethod
    def transform(self) -> DataFrame:
        pass

    def preview(self) -> DataFrame:

        with clock(f'data task execution plan: {type(self).__name__}'):
            logger.debug("Reading %s...", type(self).__name__)
            self.read()
            logger.debug("Transforming %s...", type(self).__name__)
            df = self.transform()
            logger.debug("Preview complete %s!", type(self).__name__)
            return df