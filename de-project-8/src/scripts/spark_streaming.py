from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
import pyspark.sql.functions as F

TOPIC_NAME_OUT = 'student.topic.cohort12.atsinam.out' 
TOPIC_NAME_IN = 'student.topic.cohort12.atsinam'

spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

incomming_message_schema = StructType([
    StructField('restaurant_id', StringType(), True),
    StructField('adv_campaign_id', StringType(), True),
    StructField('adv_campaign_content', LongType(), True),
    StructField('adv_campaign_owner', StringType(), True),
    StructField('adv_campaign_owner_contact', StringType(), True),
    StructField('adv_campaign_datetime_start', TimestampType(), True),
    StructField('adv_campaign_datetime_end', TimestampType(), True),
    StructField('datetime_created', TimestampType(), True)
])

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

def foreach_batch_function(df, epoch_id):

    df = df.cache()

    url = "jdbc:postgresql://localhost:5432/de"
    properties = {
        "user": "jovyan",
        "password": "jovyan",
        "driver": "org.postgresql.Driver"
    }
    
    df_pg = df.withColumn('feedback', F.lit(None).cast("string"))
    df_pg.write.jdbc(url=url, table="subscribers_feedback", mode="append", properties=properties)
   
    df_k =  (df.withColumn('value', F.to_json(
                F.struct(
                    F.col('restaurant_id'),
                    F.col('adv_campaign_id'),
                    F.col('adv_campaign_content'),
                    F.col('adv_campaign_datetime_start'),
                    F.col('adv_campaign_datetime_end'),
                    F.col('datetime_created'),
                    F.col('client_id'),
                    F.col('trigger_datetime_created')
                    )) 
                    )
            )

    (df_k.write
        .format("kafka")
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .options(**kafka_security_options) \
        .option("topic", TOPIC_NAME_OUT)
        .save()
    )

    df.unpersist() 



# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# читаем из топика Kafka сообщения с акциями от ресторанов 
restaurant_read_stream_df = (spark.readStream 
    .format('kafka') 
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .options(**kafka_security_options) \
    .option("failOnDataLoss", False) \
    .option("startingOffsets", "latest") \
    .option('subscribe', TOPIC_NAME_IN)
    .load()
)

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = (restaurant_read_stream_df 
    .withColumn('value', F.col('value').cast('string')) 
    .withColumn('value', F.from_json(col=F.col('value'), schema=incomming_message_schema))
    .where(F.current_timestamp().between(F.col('value.adv_campaign_datetime_start'), F.col('value.adv_campaign_datetime_end'))) 
    .withColumn('adv_campaign_datetime_start', F.unix_timestamp('value.adv_campaign_datetime_start'))
    .withColumn('adv_campaign_datetime_end', F.unix_timestamp('value.adv_campaign_datetime_end'))
    .withColumn('datetime_created', F.unix_timestamp('value.datetime_created'))
    .selectExpr('value.restaurant_id',
                'value.adv_campaign_id',
                'value.adv_campaign_content',
                'value.adv_campaign_owner',
                'value.adv_campaign_owner_contact',
                'adv_campaign_datetime_start',
                'adv_campaign_datetime_end',
                'datetime_created',
                'timestamp'
                )
)


# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                    .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'subscribers_restaurants') \
                    .option('user', 'student') \
                    .option('password', 'de-student') \
                    .load()

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = filtered_read_stream_df.join(subscribers_restaurant_df, 'restaurant_id', 'inner') \
            .dropDuplicates(['restaurant_id', 'adv_campaign_id', 'adv_campaign_content', 'client_id']) \
            .withWatermark('timestamp', "10 minute") \
            .withColumn('trigger_datetime_created', F.lit(current_timestamp_utc)) \
            .drop('timestamp') \
            .drop('id')

# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()