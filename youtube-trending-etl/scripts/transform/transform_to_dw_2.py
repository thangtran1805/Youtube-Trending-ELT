import duckdb
import s3fs
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from dotenv import load_dotenv

# Config JAVA
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ['PATH']

# Load credentials
load_dotenv()
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
s3_bucket = os.getenv('AWS_BUCKET_NAME')
s3_prefix = 'process_videos'

def list_all_parquet_files():
    fs = s3fs.S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)
    fullpath = f'{s3_bucket}/{s3_prefix}'
    return [f's3a://{f}' for f in fs.ls(fullpath) if f.endswith('.parquet')]

def create_spark_session():
    return SparkSession.builder \
        .appName('Transform Fact Views') \
        .config('spark.jars', '/home/thangtranquoc/jars/hadoop-aws-3.3.4.jar,/home/thangtranquoc/jars/aws-java-sdk-bundle-1.11.1026.jar') \
        .config('spark.hadoop.fs.s3a.access.key', aws_access_key_id) \
        .config('spark.hadoop.fs.s3a.secret.key', aws_secret_access_key) \
        .config('spark.hadoop.fs.s3a.endpoint', 's3.amazonaws.com') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
        .getOrCreate()

def process_file(file_path, spark, conn):
    print(f"\n📥 Đang xử lý file: {file_path}")
    df = spark.read.parquet(file_path)

    if df.rdd.isEmpty():
        print(f"⚠️ Bỏ qua file {file_path} vì không có dữ liệu.")
        return

    # Chuyển trending_date từ "yy.dd.MM" sang DATE hợp lệ
    df = df.withColumn('trending_date', F.to_date('trending_date', 'yy.dd.MM'))

    # Lấy video_sk từ DuckDB
    video_lookup = conn.execute("SELECT video_sk, video_id FROM dim_video").df()
    if video_lookup.empty:
        print("❌ Bảng dim_video trống, không thể tiếp tục.")
        return

    video_lookup_spark = spark.createDataFrame(video_lookup)

    df_joined = df.join(video_lookup_spark, on='video_id', how='inner')

    # Chuẩn hóa schema + bỏ NULL
    df_fact = df_joined.select(
        'video_sk', 'trending_date', 'views', 'likes', 'dislikes',
        'comment_count', 'country_code'
    ).dropna(subset=['video_sk', 'trending_date', 'views'])

    # Bỏ trùng theo PK
    df_fact = df_fact.dropDuplicates(['video_sk', 'trending_date'])

    # Thống kê
    print(f"🧾 Tổng số dòng gốc: {df.count()}")
    print(f"🔁 Sau join video: {df_joined.count()} dòng")
    print(f"📦 Sau khi lọc NULL & duplicate: {df_fact.count()} dòng")

    if df_fact.count() == 0:
        print(f"⚠️ Không có dòng hợp lệ để insert từ {file_path}")
        return

    # Insert vào DuckDB
    conn.register('fact_views_df', df_fact.toPandas())
    conn.execute("""
        INSERT INTO fact_views (
            video_sk, trending_date, views, likes, dislikes, comment_count, country_code
        )
        SELECT * FROM fact_views_df
        ON CONFLICT(video_sk, trending_date) DO NOTHING
    """)
    print("✅ Đã insert vào bảng fact_views.")

def transform_to_dw_2():
    spark = create_spark_session()
    conn = duckdb.connect('/home/thangtranquoc/youtube-project/youtube-trending-etl/datawarehouse.duckdb')
    parquet_files = list_all_parquet_files()

    for file in parquet_files:
        try:
            process_file(file, spark, conn)
        except Exception as e:
            print(f"❌ Lỗi xử lý file {file}: {str(e)}")

    conn.close()
    spark.stop()
    print("\n🎉 Hoàn tất insert bảng fact_views.")

# Run
transform_to_dw_2()
