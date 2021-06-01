import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, LongType, DateType
from pyspark.sql.window import Window as W


config = configparser.ConfigParser()
config.read('dl.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Create and return a spark session object
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Extract, Load and Transform song data from S3, generating
    dimension tables songs and artists, and storing the dimension
    tables in S3 as parquet files (with songs partitioned by year
    and then artist)

    INPUTS:
        spark (object)       : a spark session object
        input_data (string)  : the AWS S3 path to the original song data
        output_data (string) : the AWS S3 storage path for the dimension tables

    OUTPUTS:
        None
    '''
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("song_data_view")
    songs_table = spark.sql('''
                      SELECT DISTINCT song_id, title, artist_id, year, duration
                      FROM song_data_view
                      WHERE song_id IS NOT NULL
                  ''') 
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'), 'overwrite')

    # extract columns to create artists table
    artists_table = spark.sql('''
                        SELECT DISTINCT artist_id, 
                          artist_name AS name,
                          artist_location AS location,
                          artist_latitude AS latitude,
                          artist_longitude AS longitude
                        FROM song_data_view
                        WHERE artist_id IS NOT NULL
                     ''')
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    '''
    Extract, Load and Transform log data from S3, generating
    dimension tables users and time, generating fact table songplays, 
    and storing these dimension and fact tables in S3 as parquet files 
    (with times and songplays both partitioned by year and then month)

    INPUTS:
        spark (object)       : a spark session object
        input_data (string)  : the AWS S3 path to the original log data
        output_data (string) : the AWS S3 storage path for the dimension 
                               and fact tables

    OUTPUTS:
        None
    '''
    log_data = os.path.join(input_data, 'log-data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data) 
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong') 

    # extract columns for users table    
    df.createOrReplaceTempView("log_data_view")
    users_table = spark.sql('''
                      SELECT DISTINCT a.userId AS user_id, a.firstName AS first_name,
                        a.lastName AS last_name, a.gender, a.level
                      FROM log_data_view a
                      WHERE a.userId IS NOT NULL
                         AND a.ts = (SELECT MAX(b.ts)
                                     FROM log_data_view b
                                     WHERE b.userId = a.userId)
                  ''') 
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('timestamp', get_timestamp(df.ts))

    # # extract columns to create time table
    df.createOrReplaceTempView("log_data_view2")
    time_table = spark.sql('''
                    SELECT DISTINCT timestamp AS start_time,
                      HOUR(timestamp) AS hour,
                      DAY(timestamp) AS day,
                      WEEKOFYEAR(timestamp) AS week,
                      MONTH(timestamp) AS month,
                      YEAR(timestamp) AS year,
                      DAYOFWEEK(timestamp) AS weekday
                    FROM log_data_view2
                    WHERE timestamp IS NOT NULL
                 ''') 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time'), 'overwrite')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
                         SELECT a.timestamp AS start_time, 
                           a.userId AS user_id,
                           a.level, b.song_id, b.artist_id,
                           a.sessionId AS session_id,
                           a.location AS location, 
                           a.useragent AS user_agent,
                           YEAR(a.timestamp) AS year,
                           MONTH(a.timestamp) AS month
                         FROM log_data_view2 a INNER JOIN song_data_view b
                           ON a.artist = b.artist_name
                           AND a.song = b.title
                           AND a.length = b.duration
                         WHERE a.page = 'NextSong'
                           AND a.timestamp IS NOT NULL
                           AND a.userid IS NOT NULL
                  ''')
    
    ## ADD UNIQUE/INCREMENTING 'songplay_id' COLUMN:
    # (From https://stackoverflow.com/questions/53082891/adding-a-unique-consecutive-row-number-to-dataframe-in-pyspark/67474910#67474910
    #   and https://stackoverflow.com/questions/48209667/using-monotonically-increasing-id-for-assigning-row-number-to-pyspark-datafram)
    songplays_table= songplays_table.withColumn("new_column", lit('A'))
    w = W().partitionBy('new_column').orderBy(lit('A'))
    songplays_table = songplays_table.withColumn('songplay_id', row_number().over(w)).drop("new_column")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays'), 'overwrite')


def main():
    '''
    Create spark session, run process_song_data() and process_log_data()
    to generate parquet-formatted fact table songplays and dimension
    tables users, songs, artists, and time, and to store all tables on S3
    '''
    spark = create_spark_session()

    # From https://knowledge.udacity.com/questions/467644
    spark.sparkContext._jsc.hadoopConfiguration() \
      .set( "mapreduce.fileoutputcommitter.algorithm.version", \
            "2" \
          )

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://gapawsbucket/"
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
