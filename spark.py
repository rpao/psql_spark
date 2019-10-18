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
        files = ['queries/q1.sql', 'queries/q2.sql', 'queries/q3.sql', 'queries/q4.sql']
        numIter = 2

        print('Start Spark...')
        
        ## para cada arquivo
        for sqlFile in files:
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

            df_customer.createOrReplaceTempView("customer")
            df_date.createOrReplaceTempView("date")
            df_lineorder.createOrReplaceTempView("lineorder")
            df_part.createOrReplaceTempView("part")
            df_supplier.createOrReplaceTempView("supplier")

            ## load file
            queries = loadSql(sqlFile)

            ## for eache query in file
            q = 1
            for query in queries:
                ## report file
                dt = str(datetime.datetime.now()).split('.')[0].replace('-','').replace(' ','').replace(':','')
                nameFile = 'relatorios/spark/relatorio_' + sqlFile.split('/')[1].split('.')[0] + str(q) + '_' + dt +'.csv'
                finalFile = open(nameFile, "w")

                ## for numIter iterations
                for i in range(0,numIter):
                    dtStart = datetime.datetime.now()

                    dtEnd = datetime.datetime.now()
                    finalFile.write(str(i) + ',' + str(dtStart).split('.')[0] + ',' + str(dtEnd).split('.')[0] + '\n')
                    spark.sql(query)

                q += 1        
                finalFile.close()
            spark.stop()
        
        print("End Spark...")

    except KeyboardInterrupt:
        print("aborting...")
    except Exception as e:
        print(e)
    finally:
        exit()