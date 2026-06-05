from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, dayofmonth, 
    hour, when, isnan, count, lit
)
import os

os.environ['HADOOP_HOME'] = r'C:\hadoop'

spark = SparkSession.builder \
    .appName("DWD_Clean") \
    .config("spark.sql.warehouse.dir", r"C:\Users\26817\Desktop\data-warehouse\warehouse") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=== DWD层：数据清洗 ===")

# 读取ODS层数据
ods_path = r"C:\Users\26817\Desktop\data-warehouse\warehouse\ods\user_behavior"
df = spark.read.parquet(ods_path)

print(f"ODS层原始数据：{df.count()}行")

# 1. 数据质量检测
print("\n【数据质量报告】")
for col_name in df.columns:
    null_count = df.filter(col(col_name).isNull()).count()
    print(f"  {col_name}: {null_count}个空值")

# 2. 清洗规则
df_clean = df \
    .filter(col("user_id").isNotNull()) \
    .filter(col("product_id").isNotNull()) \
    .filter(col("event_type").isNotNull()) \
    .filter(col("price") > 0) \
    .filter(col("event_type").isin(["view", "cart", "purchase", "remove_from_cart"])) \
    .dropDuplicates(["user_id", "product_id", "event_time", "event_type"])

# 3. 字段标准化 - 拆分时间维度
df_clean = df_clean \
    .withColumn("event_date", col("event_time").cast("date")) \
    .withColumn("event_year", year(col("event_time"))) \
    .withColumn("event_month", month(col("event_time"))) \
    .withColumn("event_day", dayofmonth(col("event_time"))) \
    .withColumn("event_hour", hour(col("event_time"))) \
    .withColumn("is_purchase", when(col("event_type") == "purchase", 1).otherwise(0)) \
    .withColumn("is_cart", when(col("event_type") == "cart", 1).otherwise(0))

clean_count = df_clean.count()
print(f"\n清洗后数据：{clean_count}行")
print(f"过滤掉：{df.count() - clean_count}行异常数据")

# 4. 保存DWD层
dwd_path = r"C:\Users\26817\Desktop\data-warehouse\warehouse\dwd\user_behavior_clean"
df_clean.write.mode("overwrite").partitionBy("event_year", "event_month").parquet(dwd_path)

print(f"\nDWD层数据已写入：{dwd_path}")
print("按年月分区存储完成")
print("DWD层完成！")

spark.stop()