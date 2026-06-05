# 电商用户行为数据仓库

基于 PySpark 的电商用户行为数据仓库项目，实现 ODS→DWD→DWS→ADS 四层数仓架构。

## 数据集

Kaggle 电商行为数据集（2019-Oct），**4,244万条**真实交易记录，包含用户浏览、加购、购买行为。

- 数据来源：[eCommerce behavior data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
- 字段：event_time / event_type / product_id / category_id / brand / price / user_id

## 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| PySpark | 4.1.2 | 分布式数据处理 |
| Java | 17 | Spark运行环境 |
| Parquet | - | 列式存储格式 |
| Python | 3.14 | 脚本开发 |

## 数仓分层架构
原始CSV
↓
ODS层（原始数据接入）  →  Parquet格式存储
↓
DWD层（数据清洗）      →  按年月分区，过滤10万条异常数据
↓
DWS层（聚合汇总）      →  用户日行为650万条、商品日销售232万条
↓
ADS层（报表输出）      →  漏斗分析、品牌榜、活跃时段、用户分层
## 数据质量发现

| 字段 | 空值数量 | 占比 |
|------|---------|------|
| category_code | 13,515,609 | 31.8% |
| brand | 6,113,008 | 14.4% |
| user_session | 2 | 0% |

## 核心分析结论

**用户行为漏斗**
- 总用户：302万
- 浏览→加购转化率：11.16%
- 加购→购买转化率：102.98%（含直接购买未加购用户）

**品牌销售排名**
- Apple：收入1.11亿，均价778元，买家6.5万
- Samsung：购买量17.2万，收入4640万
- Xiaomi：收入919万，均价162元

**用户活跃时段**
- 峰值：16-17点，活跃用户43万
- 低谷：凌晨4-7点，活跃用户6-11万

**用户价值分层**
- 高价值（购买≥5次且消费≥500）：2.4万人
- 中价值（购买2-4次）：10.5万人
- 低价值（仅购买1次）：21.6万人

## 项目结构
data-warehouse/
├── src/
│   ├── ods/
│   │   └── ods_load.py       # ODS层：CSV→Parquet
│   ├── dwd/
│   │   └── dwd_clean.py      # DWD层：清洗+分区
│   ├── dws/
│   │   └── dws_agg.py        # DWS层：多维聚合
│   └── ads/
│       └── ads_report.py     # ADS层：报表输出
├── docker-compose.yml
├── .gitignore
└── README.md
## 运行方式

```bash
# 设置环境变量
$env:HADOOP_HOME = "C:\hadoop"

# 依次运行四层
python src/ods/ods_load.py
python src/dwd/dwd_clean.py
python src/dws/dws_agg.py
python src/ads/ads_report.py
```