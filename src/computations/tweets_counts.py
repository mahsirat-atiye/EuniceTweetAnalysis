from pyspark.sql import DataFrame

from src.computations.schema import Tweets, ProjectMetrics
from src.computations.task import DataTask
from src.read_write.reader_writer import ReaderWriter

from pyspark.sql import functions as f


class TweetsCount(DataTask):
    tweets: DataFrame

    def read(self):
        self.tweets = ReaderWriter.read_table(
            jc=self.jc, data_model=Tweets
        )

    def transform(self) -> DataFrame:
        """
        Compute two project metrics
        1. tweets_count: Count the total number of tweets for each project per day
        2. unique_tweets_count: count the number of unique tweets for each project on each day
            e.g. edited tweets should be counted once

        :return: DataFrame
        spark dataframe [project, time, tweets_count, unique_tweets_count]

        """

        df = self.tweets
        # create formulas for counting
        aggs = [

            f.count(f.col(Tweets.id.name)).alias(ProjectMetrics.tweets_count.name),
            f.countDistinct(f.col(Tweets.edit_history_tweet_ids.name)).alias(ProjectMetrics.unique_tweets_count.name),

        ]

        # group by time(date), project, create metrics
        df = df.withColumn('time', f.date_trunc('day', f.col(Tweets.created_at.name)))
        grouped = df.groupBy('project', 'time')
        df = grouped.agg(*aggs).orderBy('project', 'time')
        return df
