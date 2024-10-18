# Initialize Spark session
spark = SparkSession.builder \
    .appName("Monthly Sales Analysis") \
    .getOrCreate()

# Paths to your CSV files in HDFS
fact_sales_path = "hdfs://localhost:8020/user/data_source/Fact_Sales.csv"
dim_date_path = "hdfs://localhost:8020/user/data_source/Dim_date.csv"

# Load CSV files into DataFrames
fact_sales_df = spark.read.csv(fact_sales_path, header=True, inferSchema=True)
dim_date_df = spark.read.csv(dim_date_path, header=True, inferSchema=True)

# Create temporary views for SQL queries
fact_sales_df.createOrReplaceTempView("fact_sales")
dim_date_df.createOrReplaceTempView("dim_date")

# Number of 'd_x' columns (assuming 1000 columns)
num_days = 1000

# Dynamically generate the CASE statement for sales_on_day
case_statements = "CASE " + " ".join(
    [f"WHEN dd.d = 'd_{i}' THEN fs.d_{i} " for i in range(1, num_days + 1)]
) + " END AS sales_on_day"

# Construct the SQL query dynamically
sql_query = f"""
WITH MonthlySales AS (
    SELECT
        dd.date, -- Using the full date from dim_date
        fs.sale_id,
        fs.store_id,
        fs.item_id,
        fs.sell_price,
        {case_statements} -- Dynamically generated case statements for sales_on_day
    FROM 
        fact_sales fs
    JOIN 
        dim_date dd ON dd.d LIKE 'd_%' -- Ensure only valid day columns are considered
)

SELECT
    YEAR(date) AS year,
    MONTH(date) AS month,
    SUM(sell_price * sales_on_day) AS total_sales
FROM 
    MonthlySales
WHERE 
    sales_on_day > 0 -- Filter for actual sales
GROUP BY 
    YEAR(date), MONTH(date)
ORDER BY 
    year, month
"""

# Execute the SQL query
result_df = spark.sql(sql_query)

# Show the result
result_df.show()

# Stop the Spark session
spark.stop()