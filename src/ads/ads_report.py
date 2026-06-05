from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, countDistinct,
    round, when, row_number, desc
)
from pyspark.sql.window import Window
import os

os.environ['HADOOP_HOME'] = r'C:\hadoop'

spark = SparkSession.builder \
    .appName("ADS_Report") \
    .config("spark.sql.warehouse.dir", r"C:\Users\26817\Desktop\data-warehouse\warehouse") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=== ADS层：数据报表 ===")

dwd_path = r"C:\Users\26817\Desktop\data-warehouse\warehouse\dwd\user_behavior_clean"
df = spark.read.parquet(dwd_path)

# 1. 用户漏斗分析
print("\n【1. 用户行为漏斗】")
funnel = df.groupBy("user_id").agg(
    sum(when(col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
    sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
    sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count")
)

total_users = funnel.count()
view_users = funnel.filter(col("view_count") > 0).count()
cart_users = funnel.filter(col("cart_count") > 0).count()
purchase_users = funnel.filter(col("purchase_count") > 0).count()

print(f"  总用户数：{total_users:,}")
print(f"  有浏览行为：{view_users:,} ({view_users/total_users*100:.1f}%)")
print(f"  有加购行为：{cart_users:,} ({cart_users/total_users*100:.1f}%)")
print(f"  有购买行为：{purchase_users:,} ({purchase_users/total_users*100:.1f}%)")
print(f"  浏览→加购转化率：{cart_users/view_users*100:.2f}%")
print(f"  加购→购买转化率：{purchase_users/cart_users*100:.2f}%")

# 2. TOP20热销品牌
print("\n【2. TOP10热销品牌】")
brand_sales = df.filter(
    (col("event_type") == "purchase") & col("brand").isNotNull()
).groupBy("brand").agg(
    count("*").alias("purchase_count"),
    round(sum("price"), 2).alias("total_revenue"),
    round(avg("price"), 2).alias("avg_price"),
    countDistinct("user_id").alias("buyer_count")
).orderBy(desc("total_revenue"))

brand_sales.show(10, truncate=False)

# 3. 小时级用户活跃度
print("\n【3. 24小时用户活跃分布】")
hourly = df.groupBy("event_hour").agg(
    count("*").alias("event_count"),
    countDistinct("user_id").alias("active_users"),
    sum("is_purchase").alias("purchase_count")
).orderBy("event_hour")
hourly.show(24, truncate=False)

# 4. 用户价值分层（RFM简化版）
print("\n【4. 用户价值分层】")
user_value = df.filter(col("event_type") == "purchase").groupBy("user_id").agg(
    count("*").alias("frequency"),
    round(sum("price"), 2).alias("monetary"),
    round(avg("price"), 2).alias("avg_order_value")
)

high_value = user_value.filter(
    (col("frequency") >= 5) & (col("monetary") >= 500)
).count()
mid_value = user_value.filter(
    (col("frequency") >= 2) & (col("frequency") < 5)
).count()
low_value = user_value.filter(col("frequency") == 1).count()

print(f"  高价值用户（购买≥5次且消费≥500）：{high_value:,}人")
print(f"  中价值用户（购买2-4次）：{mid_value:,}人")
print(f"  低价值用户（仅购买1次）：{low_value:,}人")

# 保存ADS层核心报表
ads_path = r"C:\Users\26817\Desktop\data-warehouse\warehouse\ads"
brand_sales.write.mode("overwrite").parquet(ads_path + r"\brand_sales")
hourly.write.mode("overwrite").parquet(ads_path + r"\hourly_active")

print("\nADS层完成！")
spark.stop()