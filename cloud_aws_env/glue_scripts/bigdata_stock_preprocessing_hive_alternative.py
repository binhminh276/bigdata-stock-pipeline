import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, upper, trim, to_date, row_number, lit

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ['current_year', 'current_month', 'current_day', 'current_batch'])
year, month, day, batch = args['current_year'], args['current_month'], args['current_day'], args['current_batch']

input_s3_path = f"s3://stock-data-lake-group/raw/year={year}/month={month}/day={day}/batch={batch}/"

print(f"Tiền xử lý cho đường dẫn: {input_s3_path}")

try:
    df_raw = spark.read.json(input_s3_path)
except Exception as e:
    print(f"Không tìm thấy file JSON tại {input_s3_path}. Dừng tiến trình.")
    sys.exit(0)

window_dedup = Window.partitionBy("symbol", "trading_date", "scrape_time").orderBy(col("scrape_time").desc())

df_parsed = df_raw.withColumn("symbol_clean", upper(trim(col("symbol")))) \
                  .withColumn("trading_date_clean", to_date(col("trading_date"))) \
                  .withColumn("scrape_time_clean", col("scrape_time").cast("timestamp")) \
                  .withColumn("volume_clean", col("volume").cast("long")) \
                  .withColumn("close_clean", col("close_price").cast("double")) \
                  .withColumn("open_clean", col("open_price").cast("double")) \
                  .withColumn("high_clean", col("high_price").cast("double")) \
                  .withColumn("low_clean", col("low_price").cast("double")) \
                  .withColumn("row_num", row_number().over(window_dedup))

df_final = df_parsed.filter(col("row_num") == 1).select(
    col("symbol_clean").alias("symbol"),
    col("trading_date_clean").alias("trading_date"),
    col("scrape_time_clean").alias("scrape_time"),
    col("source"),
    col("close_clean").alias("close_price"),
    col("volume_clean").alias("volume"),
    col("open_clean").alias("open_price"),
    col("high_clean").alias("high_price"),
    col("low_clean").alias("low_price"),
    lit(year).alias("p_year"),
    lit(month).alias("p_month"),
    lit(day).alias("p_day"),
    lit(batch).alias("p_batch")
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

output_parquet_path = "s3://stock-data-lake-group/processed/stock_cleaned/"

df_final.write.mode("overwrite") \
        .partitionBy("p_year", "p_month", "p_day", "p_batch") \
        .parquet(output_parquet_path)

print(f"Hoàn tất xử lý batch {batch} ngày {day}/{month}/{year}")