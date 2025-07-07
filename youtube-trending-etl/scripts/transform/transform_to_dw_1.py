import duckdb
import s3fs
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp
import os
from dotenv import load_dotenv

# JAVA config
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ['PATH']

load_dotenv()
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
s3_bucket = os.getenv('AWS_BUCKET_NAME')
s3_prefix = 'process_videos'

def list_all_parquet_files():
    fs = s3fs.S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)
    fullpath = f'{s3_bucket}/{s3_prefix}'
    parquet_files = [f's3a://{f}' for f in fs.ls(fullpath) if f.endswith('.parquet')]
    return parquet_files

def create_spark_session():
    return SparkSession.builder \
        .appName('Transform Dim Tables') \
        .config('spark.jars', '/home/thangtranquoc/jars/hadoop-aws-3.3.4.jar,/home/thangtranquoc/jars/aws-java-sdk-bundle-1.11.1026.jar') \
        .config('spark.hadoop.fs.s3a.access.key', aws_access_key_id) \
        .config('spark.hadoop.fs.s3a.secret.key', aws_secret_access_key) \
        .config('spark.hadoop.fs.s3a.endpoint', 's3.amazonaws.com') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
        .getOrCreate()

def process_file(parquet_file_path, spark, conn):
    print(f"📥 Đang xử lý file: {parquet_file_path}")
    df = spark.read.parquet(parquet_file_path)

    if df.rdd.isEmpty():
        print(f"⚠️ File rỗng: {parquet_file_path}")
        return

    # Chuyển trending_date dạng "yy.dd.MM" thành DATE hợp lệ
    df = df.withColumn('trending_date', F.to_date('trending_date', 'yy.dd.MM'))
    total_rows = df.count()
    print(f"🧾 Tổng số dòng gốc: {total_rows}")

    ### =================== DIM_CHANNEL ===================
    dim_channel = df.select('channel_title').distinct() \
        .withColumn('channel_update_timestamp', current_timestamp())

    if dim_channel.count() > 0:
        conn.register('dim_channel_df', dim_channel.toPandas())
        conn.execute("""
            INSERT INTO dim_channel (channel_title)
            SELECT channel_title FROM dim_channel_df
            ON CONFLICT(channel_title) DO NOTHING
        """)

    channel_lookup = conn.execute("SELECT channel_id, channel_title FROM dim_channel").df()
    channel_lookup_spark = spark.createDataFrame(channel_lookup)

    ### =================== DIM_CATEGORY ===================
    dim_category = df.select('category_id', 'category_name') \
        .filter("category_id IS NOT NULL AND category_name IS NOT NULL") \
        .dropDuplicates()

    if dim_category.count() > 0:
        conn.register('dim_category_df', dim_category.toPandas())
        conn.execute("""
            INSERT INTO dim_category (category_id, category_name)
            SELECT category_id, category_name FROM dim_category_df
            ON CONFLICT(category_id) DO NOTHING
        """)

    category_lookup = conn.execute("SELECT category_id FROM dim_category").df()
    category_lookup_spark = spark.createDataFrame(category_lookup)

    ### =================== DIM_VIDEO ===================
    dim_video = df.select(
        'video_id', 'title', 'publish_time', 'tags', 'thumbnail_link', 'description',
        'comments_disabled', 'ratings_disabled', 'video_error_or_removed',
        'category_id', 'channel_title'
    ).dropDuplicates()

    dim_video = dim_video \
        .join(channel_lookup_spark, on='channel_title', how='left') \
        .join(category_lookup_spark, on='category_id', how='inner') \
        .select(
            'video_id', 'title', 'publish_time',
            'tags', 'thumbnail_link', 'description',
            'comments_disabled', 'ratings_disabled', 'video_error_or_removed',
            'channel_id', 'category_id'
        )
    dim_video = dim_video.dropDuplicates(['video_id'])
    video_rows = dim_video.count()
    print(f"🔁 Sau join dim_video: {video_rows} dòng")

    if video_rows > 0:
        conn.register('dim_video_df', dim_video.toPandas())
        conn.execute("""
            INSERT INTO dim_video (
                video_id, title, publish_time, tags, thumbnail_link, description,
                comments_disabled, ratings_disabled, video_error_or_removed,
                channel_id, category_id
            )
            SELECT 
                video_id, title, publish_time, tags, thumbnail_link, description,
                comments_disabled, ratings_disabled, video_error_or_removed,
                channel_id, category_id
            FROM dim_video_df
            ON CONFLICT(video_id) DO NOTHING
""")
    else:
        print(f"⚠️ Không có dòng dim_video hợp lệ sau join từ {parquet_file_path}")

    ### =================== DIM_TIME ===================
    # Lấy dim_time và lọc các dòng NULL
    dim_time = df.select('trending_date').dropDuplicates().filter("trending_date IS NOT NULL")

    valid_dim_time_count = dim_time.count()
    if valid_dim_time_count == 0:
        print(f"⚠️ Không có dòng dim_time hợp lệ từ {parquet_file_path}")
    else:
        print(f"📆 Số dòng dim_time hợp lệ: {valid_dim_time_count}")
    
        dim_time = df.select('trending_date') \
            .dropDuplicates() \
            .filter("trending_date IS NOT NULL") \
            .withColumn('day_of_week', F.date_format('trending_date', 'E')) \
            .withColumn('month', F.month('trending_date')) \
            .withColumn('quarter', F.quarter('trending_date')) \
            .withColumn('year', F.year('trending_date'))

        conn.register('dim_time_df', dim_time.toPandas())
        conn.execute("INSERT INTO dim_time SELECT * FROM dim_time_df ON CONFLICT(trending_date) DO NOTHING")


    print("✅ Đã load xong các bảng dim vào DuckDB.")


def transform_to_dw_1():
    parquet_files = list_all_parquet_files()
    spark = create_spark_session()
    conn = duckdb.connect('/home/thangtranquoc/youtube-project/youtube-trending-etl/datawarehouse.duckdb')

    for file in parquet_files:
        try:
            process_file(file, spark, conn)
        except Exception as e:
            print(f" Lỗi xử lý file {file}: {str(e)}")

    conn.close()
    spark.stop()
    print(" Hoàn tất load tất cả bảng dim vào DuckDB.")

# Run
transform_to_dw_1()
