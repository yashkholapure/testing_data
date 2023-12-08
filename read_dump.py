import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


try:
    conn_sql = mysql.connector.connect(
        host='xyz.com',
        user='vxyz',
        password='100',
        database='db',
        port=305
    )
    cur_sql = conn_sql.cursor(buffered=True)

    query = f"""select * from internship"""
    cur_sql.execute(query)
    conn_sql.commit()
    count = cur_sql.rowcount
    print(f"{count} rows updated.")

    rows = cur_sql.fetchall()
    columns = [col[0] for col in cur_sql.description]
    data = [tuple(row) for row in rows]
    df = spark.createDataFrame(data, columns)
    df.show(5)

    df_transformed = df.withColumn("created_at", current_timestamp())
    df_transformed.show(5)

    try:
        database_url = "jdbc://xyz.com:3306/iauro"
        table_name = "fulltime"
        properties = {
            "user": "xyz",
            "password": "100",
            "driver": "com.mysql.jdbc.Driver"  
        }
        df.write.jdbc(url=database_url, table=table_name, mode="append", properties=properties)
        print("Data dumped successfully")
    except Exception as e:
        print("Error occurred while dumping in mysql:", e)

except Exception as e:
        print("Error occurred while exicution:", e)
    

# print("Query :",query)
# df_mysql = spark.read \
#     .format("jdbc") \
#     .option("url", 'jdbc:mysql://viridium.mysql.database.azure.com:3306/viridium') \
#     .option("user", 'viridium')\
#     .option("password", 'Iauro@100')\
#     .option("driver", "com.mysql.cj.jdbc.Driver")\
#     .option("query", query)\
#     .load()
# count=df_mysql.count()
# print("Count :",count)
