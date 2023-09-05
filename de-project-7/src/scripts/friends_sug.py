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
        .appName('friend_recomendation') \
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
    
    def get_actual_geo(base_path, depth, geo):
        events = get_events(base_path, depth)
        return events.withColumn('last_event', F.row_number() \
            .over(Window.partitionBy('user_id').orderBy(F.desc('dt')))) \
            .where('last_event=1') \
            .crossJoin(geo) \
            .withColumn('d_city', distance('lat', 'lat_c', 'lon', 'lon_c')) \
            .withColumn('rank', F.row_number().over(Window.partitionBy('user_id').orderBy(F.asc('d_city')))) \
            .where('rank = 1') \
            .select('user_id', 'lon', 'lat', 'city')
    
    def get_connections(base_path, paths):
        messages = spark.read.option("basePath", base_path).parquet(*paths).where("event_type='message'and event.message_to is not null")
        return messages.selectExpr('event.message_from as user_id', 'event.message_to as conn_id')\
            .unionByName(messages.selectExpr('event.message_to as user_id', 'event.message_from as conn_id'))\
            .distinct() \
            .withColumn('filter', F.array_sort(F.array('user_id','conn_id').cast("array<string>")))
    
    def get_friends_sug(base_path, depth, actual_users_geo, conn):
        paths = get_paths(base_path, depth)
        
        subscriptions =  spark.read.parquet(*paths)\
            .where('event_type = "subscription" and lon is not null and lat is not null') \
            .selectExpr('event.user as user_id', 'event.subscription_channel') \
            .join(actual_users_geo, 'user_id', 'left') \
            .select('user_id', 'lon', 'lat', 'subscription_channel', 'city')
        
        subscriptions_r = subscriptions \
            .withColumnRenamed('user_id', 'user_id_r') \
            .withColumnRenamed('lon', 'lon_r') \
            .withColumnRenamed('lat', 'lat_r') \
            .withColumnRenamed('subscription_channel', 'subscription_channel_r') \
            .drop('city')
        
        subscriptions_cross = subscriptions.crossJoin(subscriptions_r) \
            .where('subscription_channel = subscription_channel_r and user_id != user_id_r') \
            .withColumn('filter', F.array_sort(F.array('user_id','user_id_r'))) \
            .dropDuplicates(['filter']) \
            .join(conn, 'filter', 'left_anti') \
            .drop('filter', 'subscription_channel', 'subscription_channel_r') \
            .withColumn('d', distance('lat', 'lat_r', 'lon', 'lon_r')) 
        
        return subscriptions_cross.where('d<=1').withColumn('processed_dttm', F.current_timestamp()) \
            .withColumn("TIME_UTC",F.col("processed_dttm").cast("Timestamp")) \
            .withColumn('timezone', F.concat(F.lit("Australia/"), F.col('city'))) \
            .withColumn('local_time', F.from_utc_timestamp(F.col("TIME_UTC"), \
                    F.when(F.col('timezone') == 'Australia/Mackay', 'Australia/Brisbane') \
                    .when(F.col('timezone') == 'Australia/Cairns', 'Australia/Brisbane') \
                    .when(F.col('timezone') == 'Australia/Townsville', 'Australia/Sydney') \
                    .otherwise(F.col('timezone')))) \
            .selectExpr('user_id as user_left', 'user_id_r as user_right', 'processed_dttm', 'timezone as zone_id', \
                    'local_time') 
    
    geo = get_geo()
    actual_users_geo = get_actual_geo(base_path, depth, geo)
    conn = get_connections(base_path)
    friends_sug = get_friends_sug(base_path, depth, actual_users_geo, conn)
    friends_sug.write.mode('overwrite').parquet(f'{dest_path}/date={today}')

if __name__ == "__main__":
    main()