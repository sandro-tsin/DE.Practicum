from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window 
import os
import datetime
import sys

import findspark
findspark.init()
findspark.find()

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

def main():

    depth = sys.argv[1]
    date = sys.argv[2]
    base_path = sys.argv[3]
    dest_path = sys.argv[2]

    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    today = datetime.date.today().strftime('%Y-%m-%d')


    spark = SparkSession.builder \
        .master('yarn') \
        .appName('zones_mart') \
        .getOrCreate()
    
    jvm = spark._jvm
    jsc = spark._jsc
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())

    def get_paths(base_path, depth):
        paths = [f"{base_path}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}"\
        for x in range(int(depth)) \
        if fs.exists(jvm.org.apache.hadoop.fs.\
        Path(f"{base_path}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}"))]
        return paths
    
    def distance(lat_1, lat_2, lon_1, lon_2):
        r = 6371
        sin_lat = F.pow(F.sin(F.radians(F.col(lat_2)) - F.radians(F.col(lat_1))/F.lit(2)), F.lit(2))
        cos_lat = F.cos(F.radians(F.col(lat_1))) * F.radians(F.cos(F.col(lat_2))) 
        sin_lon = F.pow(F.sin((F.radians(F.col(lon_2)) - F.radians(F.col(lon_1)))/F.lit(2)), F.lit(2))
        return 2*r*F.asin(F.sqrt(sin_lat + cos_lat * sin_lon))
    
    def get_geo():
        return spark.read.option("delimiter", ";") \
        .option('header', True) \
        .csv('/user/atsinam/analytics/geo.csv') \
        .withColumn('lat', F.regexp_replace('lat', ',', '.')) \
        .withColumn('lat', F.col('lat').cast("double")) \
        .withColumn('lng', F.regexp_replace('lng', ',', '.')) \
        .withColumn('lng', F.col('lng').cast("double"))\
        .withColumnRenamed('lng', 'lon_c')\
        .withColumnRenamed('lat', 'lat_c')
    
    def get_events(base_path, depth):
        paths = get_paths(base_path, depth)
        events = spark.read.parquet(*paths)
        
        direct = events \
            .where('event_type="message" and event.message_to is not null') \
            .selectExpr('event.message_from as user_id', 'event.message_ts as dt', 'lon', 'lat') \
            .withColumn('event_type', F.lit('message_direct'))
        
        channel = events \
            .where('event_type="message" and event.message_channel_to is not null') \
            .selectExpr('event.message_from as user_id', 'event.datetime as dt', 'lon', 'lat') \
            .withColumn('event_type', F.lit('message_channel'))
        
        subscriptions = events \
            .where("event_type = 'subscription'") \
            .selectExpr('event.user as user_id', 'event.datetime as dt', 'lon', 'lat') \
            .withColumn('event_type', F.lit('subscriptions'))

        reactions = events \
            .where("event_type = 'reaction'") \
            .selectExpr('event.reaction_from as user_id', 'event.datetime as dt', 'lon', 'lat') \
            .withColumn('event_type', F.lit('reactions'))
        
        messages = direct.union(channel)
        
        registrations = messages \
            .withColumn("message_row", F.row_number()\
                    .over(Window.partitionBy("user_id").orderBy(F.col("dt").asc()))) \
            .filter(F.col("message_row") == 1) \
            .select('user_id', 'dt', 'lon', 'lat') \
            .withColumn('event_type', F.lit('registrations'))
        
        return direct.union(channel) \
            .union(subscriptions) \
            .union(reactions) \
            .union(registrations) \
            .where('lon is not null and lat is not null')
    
    def zones_mart(events, geo):
        
        events_all_geo = events.crossJoin(geo) \
            .withColumn('d', distance('lat', 'lat_c', 'lon', 'lon_c')) \
            .withColumn('rank', F.row_number().over(Window.partitionBy('user_id', 'dt').orderBy(F.asc('d')))) \
            .where('rank = 1') \
            .select('user_id', 'dt', 'city', 'event_type')

        events_all_geo = events_all_geo\
            .withColumn('month', F.month(F.col('dt'))) \
            .withColumn('week', F.weekofyear(F.col('dt'))) \
            .withColumn('week_message', F.sum(F.when(F.col('event_type') == 'message', 1)\
                                            .when(F.col('event_type') == 'message_direct', 1).otherwise(0)) \
                        .over(Window.partitionBy('city', 'week'))) \
            .withColumn('week_reaction', F.sum(F.when(F.col('event_type') == 'reactions', 1).otherwise(0))
                        .over(Window.partitionBy('city', 'week'))) \
            .withColumn('week_subscription', F.sum(F.when(F.col('event_type') == 'subscriptions', 1).otherwise(0))
                        .over(Window.partitionBy('city', 'week'))) \
            .withColumn('week_user', F.sum(F.when(F.col('event_type') == 'registrations', 1).otherwise(0))
                        .over(Window.partitionBy('city', 'week'))) \
            .withColumn('month_message', F.sum(F.when(F.col('event_type') == 'message', 1) \
                                            .when(F.col('event_type') == 'message_direct', 1).otherwise(0))
                        .over(Window.partitionBy('city', 'month'))) \
            .withColumn('month_reaction', F.sum(F.when(F.col('event_type') == 'reactions', 1).otherwise(0))
                        .over(Window.partitionBy('city', 'month'))) \
            .withColumn('month_subscription', F.sum(F.when(F.col('event_type') == 'subscriptions', 1).otherwise(0))
                        .over(Window.partitionBy('city', 'month'))) \
            .withColumn('month_user', F.sum(F.when(F.col('event_type') == 'registrations', 1).otherwise(0))
                        .over(Window.partitionBy('city', 'month'))) 
        
        return events_all_geo.groupBy('month', 'week', 'city').agg(
                                            F.max('week_message').alias('week_message'), 
                                            F.max('week_reaction').alias('week_reaction'), 
                                            F.max('week_subscription').alias('week_subscription'), 
                                            F.max('week_user').alias('week_user'),
                                            F.max('month_message').alias('month_message'), 
                                            F.max('month_reaction').alias('month_reaction'),
                                            F.max('month_subscription').alias('month_subscription'), 
                                            F.max('month_user').alias('month_user'))
    
    events = get_events(base_path, depth)
    events = events.cache()
    geo = get_geo()
    zones_mart = zones_mart(events, geo)
    zones_mart.write.mode('overwrite').parquet(f'{dest_path}/date={today}')


if __name__ == "__main__":
        main()
