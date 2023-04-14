from time import sleep

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType


TOPIC_NAME_IN = 'student.topic.cohort8.abdurahimd.in'
TOPIC_NAME_OUT = 'student.topic.cohort8.abdurahimd.out3'

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0",
    ]
)

postgresql_subscribes_settings = {
    'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    'driver': 'org.postgresql.Driver',
    'user': 'student',
    'password': 'de-student',
    'dbtable': 'public.subscribers_restaurants'
}

postgresql_analytic_settings = {
    'url': 'jdbc:postgresql://localhost:5432/de',
    'driver': 'org.postgresql.Driver',
    'user': 'jovyan',
    'password': 'jovyan',
    'dbtable': 'public.subscribers_feedback'
}


kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
    'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
}

# определяем схему входного сообщения для json
incomming_message_schema = StructType([
    StructField("restaurant_id", StringType()),
    StructField("adv_campaign_id", StringType()),
    StructField("adv_campaign_content", StringType()),
    StructField("adv_campaign_owner", StringType()),
    StructField("adv_campaign_owner_contact", StringType()),
    StructField("adv_campaign_datetime_start", LongType()),
    StructField("adv_campaign_datetime_end", LongType()),
    StructField("datetime_created", LongType()),
])

columns = [
    'restaurant_id',
    'adv_campaign_id',
    'adv_campaign_content',
    'adv_campaign_owner',
    'adv_campaign_owner_contact',
    'adv_campaign_datetime_start',
    'adv_campaign_datetime_end',
    'datetime_created',
    'trigger_datetime_created',
    'client_id'
]

#При первом запуске ваш топик student.topic.cohort<номер когорты>.<username>.out может не существовать в Kafka и вы можете увидеть такие сообщения:
# ERROR: Topic student.topic.cohort<номер когорты>.<username>.out error: Broker: Unknown topic or partition
# Это сообщение говорит о том, что тест начал проверять работу Вашего приложение, но так как Ваше приложение ещё не отправило туда сообщения, то топик ещё не создан. Нужно подождать несколько минут.
def spark_init(name: str) -> SparkSession:
    """Создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL.

    :param name: Наименование спрак сессии.
    :return: Спарк сессия.
    :rtype: SparkSession
    """
    return (SparkSession.builder
            .appName(name)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.jars.packages", spark_jars_packages)
            .getOrCreate())


def subscribers_restaurant(spark: SparkSession, options: dict) -> DataFrame:
    """Вычитываем всех пользователей с подпиской на рестораны.

    :param spark: Рабочая спарк сессия.
    :param options: Параметры подключения к PG
    :return: Датафрейм со списком пользователей и их подписок на рестораны.
    """
    return (spark.read
            .format("jdbc")
            .options(**options)
            .load()
            .dropDuplicates(['client_id', 'restaurant_id'])
            .select('client_id', 'restaurant_id')
            )


def restaurant_event_stream(spark: SparkSession, options: dict) -> DataFrame:
    """Читаем из топика Kafka сообщения с акциями от ресторанов.

    :param spark: Рабочая спарк сессия.
    :param options: Параметры подключения к PG
    :return:
    """
    return (spark.readStream
            .format('kafka')
            .options(**options)
            .option('subscribe', TOPIC_NAME_IN)
            .load()
            )


def foreach_batch_function(df: DataFrame, epoch_id):
    """Метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров.
    """
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    # записываем df в PostgreSQL с полем feedback
    df \
        .select(columns) \
        .withColumn('feedback', F.lit('')) \
        .write.format("jdbc") \
        .mode('append') \
        .options(**postgresql_analytic_settings) \
        .save()

    # отправляем сообщения в результирующий топик Kafka без поля feedback
    df \
        .select(F.to_json(F.struct(columns)).alias('value')) \
        .write \
        .mode("append") \
        .format("kafka") \
        .options(**kafka_security_options) \
        .option("topic", TOPIC_NAME_OUT) \
        .save()

    # очищаем память от df
    df.unpersist()


def filtered_read_stream(df: DataFrame, schema: StructType) -> DataFrame:
    return (df
            .withColumn('value', F.col('value').cast(StringType()))
            .withColumn('event', F.from_json(F.col('value'), schema))
            .selectExpr('event.*')
            .withColumn('timestamp',
                        F.from_unixtime(F.col('datetime_created'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
            .dropDuplicates(['restaurant_id', 'timestamp'])
            .withWatermark('timestamp', '1 minutes')
            .withColumn('trigger_datetime_created',
                        F.unix_timestamp(F.current_timestamp()))
            .filter('trigger_datetime_created >= adv_campaign_datetime_start and adv_campaign_datetime_end >= trigger_datetime_created')
            )


def join(event_df: DataFrame, subscribers_df: DataFrame) -> DataFrame:
    """Джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid).
    """
    return (event_df
            .join(subscribers_df, 'restaurant_id', how='inner')
            )


if __name__ == "__main__":
    spark = spark_init('join stream')

    subscribers_restaurant_df  = subscribers_restaurant(spark, postgresql_subscribes_settings)
    restaurant_event_stream_df = restaurant_event_stream(spark, kafka_security_options)

    filtered_read_stream_df = filtered_read_stream(restaurant_event_stream_df, incomming_message_schema)
    joined_df = join(filtered_read_stream_df, subscribers_restaurant_df)

    query = joined_df \
        .writeStream \
        .option("checkpointLocation", "test_query4") \
        .trigger(processingTime="15 seconds") \
        .foreachBatch(foreach_batch_function) \
        .start()

    while query.isActive:
        print(f"query information: runId={query.runId}, "
              f"status is {query.status}, "
              f"recent progress={query.recentProgress}")
        sleep(60)

    query.awaitTermination()
