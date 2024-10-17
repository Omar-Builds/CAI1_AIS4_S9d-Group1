create database SalesDB;
use salesdb;
-- Create the dim_store table
CREATE TABLE dim_store (
    store_id VARCHAR(10) PRIMARY KEY,
    store_name VARCHAR(50),
    state_id VARCHAR(10)
);

-- Create the dim_product table
CREATE TABLE dim_product (
    item_id VARCHAR(20) PRIMARY KEY,
    dept_id VARCHAR(20),
    cat_id VARCHAR(20)
);

-- Create the dim_date table
CREATE TABLE dim_date (
    date_id INT PRIMARY KEY IDENTITY(1,1),
    wm_yr_wk INT,
    date DATE,
    weekday VARCHAR(10),
    wday INT,
    month INT,
    year INT,
    event_name_1 VARCHAR(50),
    event_type_1 VARCHAR(50),
    snap_CA BIT,
    snap_TX BIT,
    snap_WI BIT
);

-- Create the fact_sales table
CREATE TABLE fact_sales (
    sale_id INT PRIMARY KEY IDENTITY(1,1),
    store_id VARCHAR(10),
    item_id VARCHAR(20),
    date_id INT,
    sales_qty INT,
    sell_price DECIMAL(10, 2),
    d_1 INT DEFAULT 0,
    d_2 INT DEFAULT 0,
    d_3 INT DEFAULT 0,
    d_4 INT DEFAULT 0,
    d_5 INT DEFAULT 0,
    d_6 INT DEFAULT 0,
    d_7 INT DEFAULT 0,
    d_8 INT DEFAULT 0,
    d_9 INT DEFAULT 0,
    d_10 INT DEFAULT 0,
    d_11 INT DEFAULT 0,
    d_12 INT DEFAULT 0,
    d_13 INT DEFAULT 0,
    d_14 INT DEFAULT 0,
    d_15 INT DEFAULT 0,
    d_16 INT DEFAULT 0,
    d_17 INT DEFAULT 0,
    d_18 INT DEFAULT 0,
    d_19 INT DEFAULT 0,
    d_20 INT DEFAULT 0,
    
    -- Foreign Key constraints inside the table definition
    CONSTRAINT fk_store FOREIGN KEY (store_id) REFERENCES dim_store(store_id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_item FOREIGN KEY (item_id) REFERENCES dim_product(item_id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES dim_date(date_id) ON DELETE CASCADE ON UPDATE CASCADE
);
INSERT INTO dim_store (store_id, store_name, state_id)
VALUES 
    ('CA_1', 'Store California 1', 'CA'),
    ('TX_1', 'Store Texas 1', 'TX'),
    ('WI_1', 'Store Wisconsin 1', 'WI');

	INSERT INTO dim_product (item_id, dept_id, cat_id)
VALUES 
    ('FOOD_1', 'FOODS', 'FOOD'),
    ('HOBBY_1', 'HOBBIES', 'HOBBY'),
    ('HOUSE_1', 'HOUSEHOLD', 'HOUSE');
INSERT INTO dim_date (wm_yr_wk, date, weekday, wday, month, year, event_name_1, event_type_1, snap_CA, snap_TX, snap_WI)
VALUES
    (11325, '2013-01-01', 'Tuesday', 3, 1, 2013, 'NewYear', 'National', 1, 0, 1),
    (11326, '2013-01-02', 'Wednesday', 4, 1, 2013, NULL, NULL, 0, 1, 1),
    (11327, '2013-01-03', 'Thursday', 5, 1, 2013, NULL, NULL, 1, 0, 0);
INSERT INTO fact_sales (store_id, item_id, date_id, sales_qty, sell_price, d_1, d_2, d_3, d_4, d_5)
VALUES 
    ('CA_1', 'FOOD_1', 1, 100, 2.50, 12, 15, 0, 0, 4),
    ('TX_1', 'HOBBY_1', 2, 50, 15.00, 5, 1, 3, 0, 15),
    ('WI_1', 'HOUSE_1', 3, 75, 9.00, 0, 0, 0, 0, 1);
INSERT INTO fact_sales (store_id, item_id, date_id, sales_qty, sell_price, d_1, d_2, d_3, d_4, d_5)
VALUES 
    ('CA_1', 'FOOD_1', 1, 100, 2.50, 12, 15, 0, 0, 4),
    ('TX_1', 'HOBBY_1', 2, 50, 15.00, 5, 1, 3, 0, 15),
    ('WI_1', 'HOUSE_1', 3, 75, 9.00, 0, 0, 0, 0, 1);

	SELECT store_id, SUM(sales_qty) AS total_sales
FROM fact_sales
GROUP BY store_id;

SELECT item_id, AVG(sell_price) AS avg_price
FROM fact_sales
GROUP BY item_id;

SELECT date_id, d_1, d_2, d_3, d_4, d_5
FROM fact_sales
WHERE item_id = 'FOOD_1';

SELECT store_id, SUM(d_1) AS day_1_sales, SUM(d_2) AS day_2_sales, SUM(d_3) AS day_3_sales, SUM(d_4) AS day_4_sales, SUM(d_5) AS day_5_sales
FROM fact_sales
GROUP BY store_id;

SELECT s.state_id, SUM(f.sales_qty) AS total_sales
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
GROUP BY s.state_id;

SELECT store_id, item_id, SUM(sales_qty) AS total_sales
FROM fact_sales
GROUP BY store_id, item_id
ORDER BY total_sales DESC;

SELECT d.date, SUM(f.sales_qty) AS total_sales
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.date;
