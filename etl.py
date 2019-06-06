import configparser
from datetime import datetime
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Double, StringType as String, IntegerType as Int, DateType as date , TimestampType as Timestamp



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Description: This function can be used to populate data into songs and artists tables.

       Arguments: 
       spark: database object. 
       input_data: 'reads file path'
       output_data:'write the files'

         """
    
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    #Songs Table
    df = spark.read.json(song_data)
    df = df.distinct()
    songs_table = df.select("song_id","title","artist_id","year","duration")
    songs_table = songs_table.distinct()
    songs_table = songs_table.write.partitionBy("year","artist_id").format("parquet").save(os.path.join(output_data,"songs.parquet"))

    # artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude")
    artists_table = artists_table.distinct()
    artists_table = artists_table.write.format("parquet").save(os.path.join(output_data,"artists.parquet"))


def process_log_data(spark, input_data, output_data):
    
    """ Description: This function can be used to populate data into users,time and songs_play tables.

       Arguments: 
       spark: database object. 
       input_data: reads file path
       output_data: write the files

    """
    
    log_data = input_data + 'log_data/*/*/*.json'
    log_dataSchema = R([
       Fld("artist",   String()),
   Fld("auth",  String() ),
   Fld("firstName",  String() ),
   Fld("gender",  String() ),
   Fld("itemInSession",  String() ),
   Fld("lastName",   String()),
   Fld("length",  Double() ),
   Fld("level",  String() ),
   Fld("location",  String() ),
   Fld("method",  String() ),
   Fld("page",  String() ),
   Fld("Rregistration",  String() ),
   Fld("sessionId",  String() ),
   Fld("song",  String() ),
   Fld("status",  Int()),
   Fld("ts",  Timestamp()),
   Fld("userAgent",  String()),
   Fld("userId",  Int()),
])

    # read log data file
    log_dataSchema = spark.read.json(log_data) 
    df = log_dataSchema.filter(log_dataSchema.page =='NextSong')

    # users table    
    users_table = df.select('userId','firstName','lastName','gender','level').dropDuplicates().sort("userId")
    users_table = users_table.write.format("parquet").save(os.path.join(output_data,"users_table.parquet"))

    # create timestamp 
    df_filter = log_dataSchema.filter(log_dataSchema.page =='NextSong')
    df_column = df_filter.select("ts")
    df = df_column.toPandas()
    time_data = df['ts'].apply(pd.to_datetime)
    time_data= pd.DataFrame(time_data) 
    time_data= { 'Time':time_data['ts'],'hour':time_data['ts'].dt.hour, 'day':time_data['ts'].dt.day_name(), 
                     'week of year':time_data['ts'].dt.weekofyear,'month':time_data['ts'].dt.month,
                     'year':time_data['ts'].dt.year,'weekday':time_data['ts'].dt.weekday}
    time_df = pd.DataFrame(time_data)
    time_Schema =  R([
        Fld("Time",Timestamp()),
        Fld("hour",Int()),
        Fld("day",String()),
        Fld("week_of_year",Int()),
        Fld("month",Int()),
        Fld("year",Int()),
        Fld("weekday",Int()),
    ])
    df = spark.createDataFrame(time_df,schema=time_Schema) 
    df = df.select ('Time','hour','day','week_of_year','month','year','weekday')
    tiem_table = df.write.partitionBy("year","month").format("parquet").save(os.path.join(output_data,"tiem_table.parquet"))

    # songplays table
    s_df = spark.read.parquet("Songs.parquet")
    song_df = s_df.createOrReplaceTempView("Songs_table")
    
    a_df = spark.read.parquet("artists_table.parquet")
    artist_df = a_df.createOrReplaceTempView("artists_table")
    
    path = input_data + 'log_data/*/*/*.json'
    songs_data = spark.read.json(path)
    stg_df = songs_data.createOrReplaceTempView("stg_songs")
    
    df =spark.sql(''' select distinct row_number() over (order by "sg.userId") as songplay_id,
                      sg.ts as ts,sg.userId,sg.level,b.song_id,b.artist_id,sg.sessionId,sg.location,sg.userAgent
                      from stg_songs as sg left join 
                      (
                      SELECT s.song_id,a.artist_id,a.artist_name from Songs_table as s 
                      left JOIN artists_table as a ON s.artist_id = a.artist_id)as b
                      on sg.artist = b.artist_name  where sg.page = 'NextSong' 
                      group by sg.ts,sg.userId,sg.level,b.song_id,b.artist_id,sg.sessionId,sg.location,sg.userAgent ''')


    df = df.toPandas()
    df = pd.DataFrame(df)
    t_df= pd.DataFrame({'songplay_id':df['songplay_id'],'ts':df['ts'].apply(pd.to_datetime),
                        'userId':df['userId'],'level':df['level'],
                        'song_id':df['song_id'],'artist_id':df['artist_id'],
                        'sessionId':df['sessionId'],'location':df['location'],
                        'userAgent':df['userAgent']})

    
    df = spark.createDataFrame(t_df)
    df.select('songplay_id','ts','userId','level','song_id','artist_id','sessionId','location','userAgent')
    
    songplays_table = (df.withColumn("year", year(col("ts").cast("timestamp")))
                           .withColumn("month", month(col("ts").cast("timestamp"))))
    
    songplays_data = songplays_table.write.partitionBy("year","month").format("parquet").save(os.path.join(output_data,"songplays_table.parquet"))

def main():
    """
       Description: This function can be used can be used to read 
                    the database config file and creates spark connection 
                    and completes the etl process.
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
