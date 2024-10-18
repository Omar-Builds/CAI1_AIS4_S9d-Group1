# Initialize Spark session
spark = SparkSession.builder \
    .appName("Total Sales per Store/Product") \
    .getOrCreate()

# Paths to your CSV files in HDFS
fact_sales_path = "hdfs://localhost:8020/user/data_source/Fact_Sales.csv"

# Load CSV file into DataFrame
fact_sales_df = spark.read.csv(fact_sales_path, header=True, inferSchema=True)

# Create temporary view for SQL queries
fact_sales_df.createOrReplaceTempView("fact_sales")

# Execute SQL query for total sales per store
total_sales_per_store_query = """
SELECT
    store_id,
    SUM(d_1 + d_2 + d_3 + d_4 + d_5 + d_6 + d_7 + d_8 + d_9) AS Total_sales,
    SUM(sell_price * (d_1 + d_2 + d_3 + d_4 + d_5 + d_6 + d_7 + d_8 + d_9)) AS Total_sell_price
FROM
    fact_sales
GROUP BY
    store_id
ORDER BY
    Total_sales DESC
"""

# Execute the SQL query
result_df = spark.sql(total_sales_per_store_query)

# Show the result
result_df.show()

# Stop the Spark session
spark.stop()