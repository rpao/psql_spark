import datetime

import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession

def loadSql(file_name):
    file = open(file_name, "r")
    sql = []
    for line in file.readlines():
        sql.append(line.replace(';',''))
    return sql

if __name__ == '__main__':
    try:
        dt = str(datetime.datetime.now()).split('.')[0].replace('-','').replace(' ','').replace(':','')
        nameFile = 'relatorios/spark/relatorio_'+ dt +'.csv'
        
        finalFile = open(nameFile, "w")
        finalFile.write('Spark - Starting at ' + str(datetime.datetime.now()) + '\n')

        ## create spark session
        spark = SparkSession \
            .builder \
            .appName('Postgres Spark') \
            .config('spark.jars', 'connectors/postgresql-42.2.8.jar') \
            .config('spark.executor.memory', '10g') \
            .config('spark.executor.cores', '8') \
            .config('spark.cores.max', '8') \
            .config('spark.driver.memory','10g') \
            .getOrCreate()

        ## User information
        user     = 'postgres'
        password = 'root'
        dataset = 'ssb_sf10'

        ## Database information
        url = 'jdbc:postgresql://localhost:5432/' + dataset + '?user=' + user + '&password=' + password
        properties = {'driver': 'org.postgresql.Driver', 'password': password,'user': user}
        
        df_customer = spark.read.jdbc(url=url, table='customer', properties=properties)
        df_date = spark.read.jdbc(url=url, table='date', properties=properties)
        df_lineorder = spark.read.jdbc(url=url, table='lineorder', properties=properties)
        df_part = spark.read.jdbc(url=url, table='part', properties=properties)
        df_supplier = spark.read.jdbc(url=url, table='supplier', properties=properties)

        finalFile.write('Spark - Temporaty Views at ' + str(datetime.datetime.now()) + '\n')

        df_customer.createOrReplaceTempView("customer")
        df_date.createOrReplaceTempView("date")
        df_lineorder.createOrReplaceTempView("lineorder")
        df_part.createOrReplaceTempView("part")
        df_supplier.createOrReplaceTempView("supplier")

        files = ['queries/q1.sql', 'queries/q2.sql', 'queries/q3.sql', 'queries/q4.sql']
        numIter = 1
        finalFile.write('Spark - Quering at ' + str(datetime.datetime.now()) + '\n')
        # Global temporary view is tied to a system preserved database `global_temp`
        for iter in range(0,numIter):
            for sqlFile in files:
                queries = loadSql(sqlFile)
                for query in queries:
                    spark.sql(query)
        
        spark.stop()
        finalFile.write('Spark - Ending at ' + str(datetime.datetime.now()) + '\n')
    except KeyboardInterrupt:
        print("aborting...")
        finalFile.write('Spark - aborting at ' + str(datetime.datetime.now()) + '\n')
    except Exception as e:
        print(e)
        finalFile.write('Spark - error at ' + str(datetime.datetime.now()) + '\n')
    finally:
        finalFile.close()
        exit()