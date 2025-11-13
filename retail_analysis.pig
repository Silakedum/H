
transactions = LOAD '/user/hadoop/retail/input/transactions.csv'
USING PigStorage(',')
AS (Transaction_ID:int, Date:chararray, Store_ID:chararray,
    Product_ID:chararray, Product_Category:chararray,
    Quantity:int, Unit_Price:float, Customer_ID:chararray);


transaction_revenue = FOREACH transactions GENERATE
    Transaction_ID, (Quantity * Unit_Price) AS Revenue;


category_sales = FOREACH (GROUP transactions BY Product_Category)
    GENERATE group AS Category, SUM(transactions.Quantity * transactions.Unit_Price) AS Total_Revenue;


product_sales = FOREACH (GROUP transactions BY Product_ID)
    GENERATE group AS Product_ID, SUM(transactions.Quantity) AS Total_Quantity;
top_products = ORDER product_sales BY Total_Quantity DESC;


store_revenue = FOREACH (GROUP transactions BY Store_ID)
    GENERATE group AS Store_ID, SUM(transactions.Quantity * transactions.Unit_Price) AS Total_Revenue;
sorted_store_revenue = ORDER store_revenue BY Total_Revenue DESC;


STORE category_sales INTO '/user/hadoop/retail/output/category_sales' USING PigStorage(',');
STORE top_products INTO '/user/hadoop/retail/output/top_products' USING PigStorage(',');
STORE sorted_store_revenue INTO '/user/hadoop/retail/output/store_revenue' USING PigStorage(',');
