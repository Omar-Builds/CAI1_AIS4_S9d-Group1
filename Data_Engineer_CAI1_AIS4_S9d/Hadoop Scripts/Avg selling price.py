# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("Average Selling Price by Category") \
    .getOrCreate()

# Paths to your CSV files in HDFS
fact_sales_path = "hdfs://localhost:8020/user/data_source/Fact_Sales.csv"
dim_product_path = "hdfs://localhost:8020/user/data_source/Dim_product.csv"

# Load CSV files into DataFrames
fact_sales_df = spark.read.csv(fact_sales_path, header=True, inferSchema=True)
dim_product_df = spark.read.csv(dim_product_path, header=True, inferSchema=True)

# Perform the join between fact_sales and dim_product
joined_df = fact_sales_df.join(dim_product_df, fact_sales_df.item_id == dim_product_df.item_id)

# Calculate the average selling price by category
avg_price_by_category_df = joined_df.groupBy("cat_id").agg(avg("sell_price").alias("avg_price"))

# Order by average price in descending order
result_df = avg_price_by_category_df.orderBy("avg_price", ascending=False)

# Show the result
result_df.show()

# Stop the SparkSession
spark.stop()