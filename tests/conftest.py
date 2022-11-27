"""
Note that "conftest.py" is a special name for pytest, which will automatically make available the fixtures defined here
to all the tests in the "tests" directory.
"""
import pytest as pytest
from unittest import mock
from pyspark.sql import SparkSession

from src.utils.job_context import get_spark


@pytest.fixture()
def spark() -> SparkSession:
    """Use this fixture to directly get a handle of the Spark session"""
    return get_spark()


@pytest.fixture()
def m_read_table():
    with mock.patch('src.read_write.reader_writer.ReaderWriter.read_table') as _mock:
        yield _mock

