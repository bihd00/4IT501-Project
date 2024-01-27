from pyodbc import Connection
from pandas import DataFrame
from pandas import read_sql


def get_orders_all_years(conn: Connection) -> DataFrame:
    sql = f'''
        SELECT
            Row_ID,
            Order_ID,

            Category,
            Sub_Category,
            Segment,

            Country_Region,
            State_Province,
            Region,
            City,
            Postal_Code,

            Customer_ID,
            Customer_Name,

            Product_ID,
            Product_Name,

            Quantity,
            Sales,
            Discount,
            Profit,

            Order_Date,
            Ship_Date,
            Ship_Mode

        FROM
        (
            --based on excel "analysis"
            --2020
            SELECT
                Category,
                City,
                Country_Region,
                Customer_ID,
                Customer_Name,
                CAST(REPLACE(Discount, ',', '.') AS FLOAT) AS Discount,
                Order_ID,
                DATEFROMPARTS(Order_Year, Order_month, Order_day) AS Order_Date,
                Postal_Code,
                Product_ID,
                Product_Name,
                CAST(REPLACE(Profit, ',', '.') AS FLOAT) AS Profit,
                CAST(Quantity AS INT) AS Quantity,
                Region,
                CAST(Row_ID AS INT) AS Row_ID,
                CAST(Sales AS FLOAT) AS Sales,
                Segment,
                CAST(Ship_Date as DATE) as Ship_Date,
                Ship_Mode,
                State_Province,
                Sub_Category
            FROM [Data-raw-Orders-2020]
            UNION ALL
            --2021
            SELECT
                Category,
                City,
                Country_Region,
                Customer_ID,
                Customer_Name,
                CAST(REPLACE(Discount, ',', '.') AS FLOAT) AS Discount,
                CONCAT(Subsidiary, '-', CAST(Order_Year AS NVARCHAR), '-', CAST(Order_ID AS NVARCHAR)) AS Order_id,
                DATEFROMPARTS(Order_Year, Order_month, Order_day) AS Order_Date,
                Postal_Code,
                Product_ID,
                Product_Name,
                CAST(REPLACE(Profit, ',', '.') AS FLOAT) AS Profit,
                CAST(Quantity AS INT) AS Quantity,
                Region,
                Row_ID,
                Sales,
                Segment,
                CAST(Ship_Date as DATE) as Ship_Date,
                Ship_Mode,
                State_Province,
                Sub_Category
            FROM [Data-raw-Orders-2021]
            UNION ALL
            --2022
            SELECT
                Category,
                City,
                Country_Region,
                Customer_ID,
                Customer_Name,
                CAST(REPLACE(Discount, ',', '.') AS FLOAT) AS Discount,
                Order_ID,
                DATEFROMPARTS(Order_Year, Order_month, Order_day) AS Order_Date,
                Postal_Code,
                Product_ID,
                Product_Name,
                CAST(REPLACE(Profit, ',', '.') AS FLOAT) AS Profit,
                CAST(Quantity AS INT) AS Quantity,
                Region,
                Row_ID,
                Sales,
                Segment,
                CAST(Ship_Date as DATE) as Ship_Date,
                Ship_Mode,
                State_Province,
                Sub_Category
            FROM [Data-raw-Orders-2022]
            UNION ALL
            --2023
            SELECT
                Category,
                City,
                Country_Region,
                Customer_ID,
                Customer_Name,
                CAST(REPLACE(Discount, ',', '.') AS FLOAT) AS Discount,
                Order_ID,
                CAST(Order_Date AS DATE) AS Order_Date,
                Postal_Code,
                Product_ID,
                Product_Name,
                Profit,
                Quantity,
                Region,
                Row_ID,
                Sales,
                Segment,
                CAST(Ship_Date as DATE) as Ship_Date,
                Ship_Mode,
                State_Province,
                Sub_Category
            FROM [Data-raw-Orders-2023]
        ) t
    '''
    return read_sql(sql=sql, con=conn)