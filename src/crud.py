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


def drop_orders(conn: Connection) -> None:
    sql = '''
    DROP TABLE IF EXISTS team_01_orders
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.commit()
    except Exception as e:
        print('[ERROR]', e)


def create_orders(conn: Connection) -> None:
    sql = '''
    CREATE TABLE team_01_orders (
        id INT PRIMARY KEY IDENTITY(1,1),
        Order_ID VARCHAR(100),
        Category VARCHAR(255),
        Sub_Category VARCHAR(255),
        Segment VARCHAR(255),
        Country_Region VARCHAR(50),
        State_Province VARCHAR(100),
        Region VARCHAR(255),
        City VARCHAR(255),
        Postal_Code VARCHAR(10),
        Customer_ID VARCHAR(20),
        Customer_Name VARCHAR(255),
        Product_ID VARCHAR(30),
        Product_Name VARCHAR(255),
        Quantity INT,
        Sales FLOAT,
        Discount FLOAT,
        Profit FLOAT,
        Order_Date DATE,
        Ship_Date DATE,
        Ship_Mode VARCHAR(20)
    )
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.commit()
    except Exception as e:
        print('[ERROR]', e)


def set_identity_on_orders_off(conn: Connection):
    sql = '''
        SET IDENTITY_INSERT team_01_orders OFF
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.commit()
    except Exception as e:
        print('[ERROR]', e)


def set_identity_on_orders_on(conn: Connection):
    sql = '''
        SET IDENTITY_INSERT team_01_orders ON
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.commit()
    except Exception as e:
        print('[ERROR]', e)


def insert_to_orders(
    conn: Connection,
    *args
) -> None:
    sql = f'''
        INSERT INTO
            team_01_orders
        (
            id,
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
        )
        VALUES
        ({("?,"*21)[:-1]})
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(sql, tuple(args))
        cursor.commit()
    except Exception as e:
        print('[ERROR]', e)


def create_quota(conn: Connection) -> None:
    sql = '''
    CREATE TABLE team_01_quota (
        id INT PRIMARY KEY IDENTITY(1,1),
        Region VARCHAR(100),
        Year INT,
        Value INT
    )
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.commit()
    except Exception as e:
        print('[ERROR]', e)



def drop_quota(conn: Connection) -> None:
    sql = '''
    DROP TABLE IF EXISTS team_01_quota
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.commit()
    except Exception as e:
        print('[ERROR]', e)


def drop_inflation(conn: Connection) -> None:
    sql = '''
    DROP TABLE IF EXISTS team_01_inflation
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.commit()
    except Exception as e:
        print('[ERROR]', e)


def create_inflation(conn: Connection) -> None:
    sql = '''
    CREATE TABLE team_01_inflation (
        id INT PRIMARY KEY IDENTITY(1,1),
        Region VARCHAR(100),
        Year INT,
        Month INT,
        Inflation FLOAT
    )
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.commit()
    except Exception as e:
        print('[ERROR]', e)


def insert_to_inflation(
    conn: Connection,
    region: str,
    year: int,
    month: int,
    inflation: float,
) -> None:
    sql = '''
        INSERT INTO
            team_01_inflation
        (
            Region,
            Year,
            Month,
            Inflation
        )
        VALUES
        (?, ?, ?, ?)
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (region, year, month, inflation))
        cursor.commit()
    except Exception as e:
        print('[ERROR]', e)


def create_returns(conn: Connection) -> None:
    sql = '''
    CREATE TABLE team_01_returns (
        id INT PRIMARY KEY IDENTITY(1,1),
        Order_ID VARCHAR(100),
        isReturned SMALLINT
    )
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.commit()
    except Exception as e:
        print('[ERROR]', e)


def drop_returns(conn: Connection) -> None:
    sql = '''
    DROP TABLE IF EXISTS team_01_returns
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.commit()
    except Exception as e:
        print('[ERROR]', e)


def insert_to_quota(
    conn: Connection,
    region: str,
    year: int,
    value: int,
) -> None:
    sql = '''
        INSERT INTO
            team_01_quota
        (
            Region,
            Year,
            Value
        )
        VALUES
        (?, ?, ?)
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (region, year, value))
        cursor.commit()
    except Exception as e:
        print('[ERROR]', e)


def insert_to_returns(
    conn: Connection,
    order_id: str,
    is_returned: bool,
) -> None:
    sql = '''
        INSERT INTO
            team_01_returns
        (
            Order_ID,
            isReturned
        )
        VALUES
        (?, ?)
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (order_id, is_returned))
        cursor.commit()
    except Exception as e:
        print('[ERROR]', e)