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
        .appName('users_geo_mart') \
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
    
    def get_messages(base_path, depth):
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
        
        return direct.union(channel) \
            .where('lon is not null and lat is not null')
    
    def users_geo_mart():   
    
            messages_city = messages.crossJoin(geo) \
                .withColumn('d', distance('lat', 'lat_c', 'lon', 'lon_c')) \
                .withColumn('rank', F.row_number().over(Window.partitionBy('user_id', 'dt').orderBy(F.asc('d')))) \
                .where('rank = 1')\
                .select('user_id', 'dt', 'city') 
            
            messages_city_act = messages_city \
                .withColumn('last_message_row_number', F.row_number() \
                        .over(Window.partitionBy('user_id').orderBy(F.desc('dt')))) \
                .where('last_message_row_number=1') \
                .withColumn("TIME_UTC",F.col("dt").cast("Timestamp")) \
                .withColumn('timezone', F.concat(F.lit("Australia/"), F.col('city'))) \
                .withColumn('local_time', F.from_utc_timestamp(F.col("TIME_UTC"), \
                        F.when(F.col('timezone') == 'Australia/Mackay', 'Australia/Brisbane') \
                        .when(F.col('timezone') == 'Australia/Cairns', 'Australia/Brisbane') \
                        .when(F.col('timezone') == 'Australia/Townsville', 'Australia/Sydney') \
                        .otherwise(F.col('timezone')))) \
                .selectExpr('user_id', 'dt', 'city as act_city', 'local_time')
            
            messages_city_home =  messages_city \
                .withColumn('date', F.to_date(F.col('dt'))) \
                .withColumn('city_row', F.row_number()\
                            .over(Window.partitionBy('user_id', 'city', 'date').orderBy('dt'))) \
                .where('city_row =1')\
                .withColumn('row', F.row_number().over(Window.partitionBy('user_id', 'city').orderBy('date'))) \
                .withColumn('day', F.date_add(F.col('date'), -F.row_number().over(Window.partitionBy('user_id', 'city').orderBy('date')))) \
                .withColumn('cons_days', F.count("*").over(Window.partitionBy('user_id', 'city', 'day').orderBy('date'))) \
                .where('cons_days=2') \
                .withColumn('cons_days_row', F.row_number() \
                            .over(Window.partitionBy('user_id', 'cons_days').orderBy(F.desc('date')))) \
                .where('cons_days_row = 1') \
                .selectExpr(' user_id', 'city as home_city') 
            
            messages_travel = messages_city\
                .withColumn('date', F.to_date(F.col('dt'))) \
                .withColumn('city_row', F.row_number()\
                            .over(Window.partitionBy('user_id', 'city', 'date').orderBy('dt'))) \
                .where('city_row = 1')\
                .orderBy('user_id', 'date') \
                .withColumn('city_lag', F.lag('city').over(Window.partitionBy('user_id').orderBy('dt'))) \
                .withColumn('travel', F.when(F.col('city') != F.col('city_lag'), 1).otherwise(0)) \
                .where('travel =1') \
                .withColumn('travel_count', F.count('city').over(Window.partitionBy('user_id'))) \
                .groupBy('user_id').agg(F.max('travel_count'), F.collect_list('city').alias('travel_array'))
            
            return messages_city_act.join(messages_travel, 'user_id', 'left') \
                .join(messages_city_home, 'user_id', 'left')
    
    messages = get_messages(base_path, depth)
    messages = messages.cache()
    geo = get_geo()
    users_geo_mart = users_geo_mart(messages)
    users_geo_mart.write.mode('overwrite').parquet(f'{dest_path}/date={today}')

if __name__ == "__main__":
    main()