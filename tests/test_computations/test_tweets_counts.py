import pytest

from chispa import assert_df_equality
from pyspark.sql import SparkSession

from datetime import datetime

from src.computations.tweets_counts import TweetsCount
from src.utils.time import parse_time

tweets_schema = ("project", "id", "created_at", "edit_history_tweet_ids")

expected_schema = ("project", "time", "tweets_count", "unique_tweets_count")

org_proj_name = "test"


@pytest.mark.parametrize(
    "tweets,expected",
    [
        (
            # tweets: "project", "id", "created_at", "edit_history_tweet_ids"
            [
                ('a', '2', parse_time('2022-11-15T16:39:41.000Z'), ['1', '2', '3']),
                ('a', '3', parse_time('2022-11-15T16:49:41.000Z'), ['1', '2', '3']),
            ],
            # expected: "project", "time", "tweets_count", "unique_tweets_count"
            [
                ('a', datetime.strptime("2022-11-15", '%Y-%m-%d'), 2, 1),

            ],
        ),
        (
                # tweets: "project", "id", "created_at", "edit_history_tweet_ids"
                [
                    ('a', '1', parse_time('2022-11-15T16:39:41.000Z'), ['1', '2']),
                    ('a', '2', parse_time('2022-11-16T16:49:41.000Z'), ['1', '2']),
                ],

                # expected: "project", "time", "tweets_count", "unique_tweets_count"
                [
                    ('a', datetime.strptime("2022-11-15", '%Y-%m-%d'), 1, 1),
                    ('a', datetime.strptime("2022-11-16", '%Y-%m-%d'), 1, 1),

                ],
        ),
        (
                # tweets: "project", "id", "created_at", "edit_history_tweet_ids"
                [
                    ('a', '1', parse_time('2022-11-15T16:39:41.000Z'), ['1']),
                    ('a', '2', parse_time('2022-11-16T16:49:41.000Z'), ['2']),
                    ('b', '3', parse_time('2022-11-16T16:49:41.000Z'), ['3']),
                ],

                # expected: "project", "time", "tweets_count", "unique_tweets_count"
                [
                    ('a', datetime.strptime("2022-11-15", '%Y-%m-%d'), 1, 1),
                    ('a', datetime.strptime("2022-11-16", '%Y-%m-%d'), 1, 1),
                    ('b', datetime.strptime("2022-11-16", '%Y-%m-%d'), 1, 1),

                ],
        ),



    ],
)
def test_unique_question_count(m_read_table, spark: SparkSession, tweets, expected):

    input_df = spark.createDataFrame(tweets, tweets_schema)
    m_read_table.return_value = input_df

    expected_df = spark.createDataFrame(expected, expected_schema)

    df = TweetsCount().preview()

    assert_df_equality(
        df,
        expected_df,
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=True,
    )
