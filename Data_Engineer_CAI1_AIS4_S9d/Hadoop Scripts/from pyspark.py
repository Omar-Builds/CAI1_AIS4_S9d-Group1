from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Dynamic Sales By Day Analysis") \
    .getOrCreate()

# Paths to your CSV files in HDFS
fact_sales_path = "hdfs://localhost:8020/user/data_source/Fact_Sales.csv"
dim_date_path = "hdfs://localhost:8020/user/data_source/dim_date.csv"

# Load CSV files into DataFrames
fact_sales_df = spark.read.csv(fact_sales_path, header=True, inferSchema=True)
dim_date_df = spark.read.csv(dim_date_path, header=True, inferSchema=True)

# Create temporary views for SQL queries
fact_sales_df.createOrReplaceTempView("fact_sales")
dim_date_df.createOrReplaceTempView("dim_date")

# Number of days (adjust as needed)
num_days = 1000

# Dynamically create the CASE statement for sales columns
case_statement = "CASE " + " ".join(
    [f"WHEN dd.d = 'd_{i}' THEN fs.d_{i}" for i in range(1, num_days + 1)]
) + " END AS sales_on_day"

# Constructing the dynamic SQL query
dynamic_query = f"""
WITH SalesByDay AS (
    SELECT
        fs.store_id,
        dd.event_name_1,
        fs.sell_price,
        {case_statement}
    FROM
        fact_sales fs
    JOIN
        dim_date dd ON dd.d LIKE 'd_%'
    WHERE
        dd.event_name_1 IS NOT NULL AND dd.event_name_1 != ''
)
SELECT
    store_id,
    event_name_1,
    SUM(sell_price * sales_on_day) AS total_sales
FROM
    SalesByDay
WHERE
    sales_on_day > 0
GROUP BY
    store_id, event_name_1
ORDER BY
    total_sales DESC
"""

# Execute the dynamic SQL query
result_df = spark.sql(dynamic_query)

# Show the result
result_df.show()

# Stop the Spark session
spark.stop()
