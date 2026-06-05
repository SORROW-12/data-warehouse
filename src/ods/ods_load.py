from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth

spark = SparkSession.builder \
    .appName("ODS_Load") \
    .config("spark.sql.warehouse.dir", r"C:\Users\26817\Desktop\data-warehouse\warehouse") \
    .config("spark.driver.memory", "4g") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=== ODS层：原始数据接入 ===")

# 读取原始CSV
df = spark.read.csv(
    r"C:\Users\26817\Desktop\2019-Oct.csv",
    header=True,
    inferSchema=True
)

print(f"原始数据总行数：{df.count()}")
print("原始Schema：")
df.printSchema()

# 保存为Parquet格式（ODS层）
output_path = r"C:\Users\26817\Desktop\data-warehouse\warehouse\ods\user_behavior"
df.write.mode("overwrite").parquet(output_path)

print(f"ODS层数据已写入：{output_path}")
print("ODS层完成！")

spark.stop()