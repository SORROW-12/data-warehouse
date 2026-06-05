from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, countDistinct,
    round, when, max, min
)
import os

os.environ['HADOOP_HOME'] = r'C:\hadoop'

spark = SparkSession.builder \
    .appName("DWS_Agg") \
    .config("spark.sql.warehouse.dir", r"C:\Users\26817\Desktop\data-warehouse\warehouse") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=== DWS层：聚合汇总 ===")

dwd_path = r"C:\Users\26817\Desktop\data-warehouse\warehouse\dwd\user_behavior_clean"
df = spark.read.parquet(dwd_path)

# 1. 用户行为日聚合表
print("\n【1. 用户日行为聚合】")
user_daily = df.groupBy("user_id", "event_date").agg(
    count("*").alias("total_events"),
    countDistinct("product_id").alias("distinct_products"),
    sum("is_purchase").alias("purchase_count"),
    sum("is_cart").alias("cart_count"),
    sum(when(col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
    round(sum(when(col("event_type") == "purchase", col("price")).otherwise(0)), 2).alias("daily_spend")
)
user_daily_path = r"C:\Users\26817\Desktop\data-warehouse\warehouse\dws\user_daily_behavior"
user_daily.write.mode("overwrite").parquet(user_daily_path)
print(f"用户日行为聚合完成：{user_daily.count()}条记录")

# 2. 商品销售日聚合表
print("\n【2. 商品日销售聚合】")
product_daily = df.groupBy("product_id", "event_date", "brand", "category_code").agg(
    sum(when(col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
    sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
    sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
    round(sum(when(col("event_type") == "purchase", col("price")).otherwise(0)), 2).alias("revenue"),
    round(avg("price"), 2).alias("avg_price")
)
product_daily_path = r"C:\Users\26817\Desktop\data-warehouse\warehouse\dws\product_daily_sales"
product_daily.write.mode("overwrite").parquet(product_daily_path)
print(f"商品日销售聚合完成：{product_daily.count()}条记录")

# 3. 月度整体指标汇总
print("\n【3. 月度整体指标】")
monthly_summary = df.groupBy("event_year", "event_month").agg(
    count("*").alias("total_events"),
    countDistinct("user_id").alias("active_users"),
    countDistinct("product_id").alias("active_products"),
    sum("is_purchase").alias("total_purchases"),
    round(sum(when(col("event_type") == "purchase", col("price")).otherwise(0)), 2).alias("total_revenue"),
    round(
        sum("is_purchase") / sum(when(col("event_type") == "view", 1).otherwise(0)) * 100, 4
    ).alias("view_to_purchase_rate")
).orderBy("event_year", "event_month")

monthly_summary.show(20, truncate=False)

monthly_path = r"C:\Users\26817\Desktop\data-warehouse\warehouse\dws\monthly_summary"
monthly_summary.write.mode("overwrite").parquet(monthly_path)
print(f"月度汇总完成")

print("\nDWS层完成！")
spark.stop()