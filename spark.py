import datetime

import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession

if __name__ == '__main__':
    try:
        print("Start Spark...")

        ## arquivos: q1.sql, q2.sql, q3.sql, q4.sql; 
        sqlfile = open('queries/q1.sql','r')
        queries = []
        for row in sqlfile.readlines():
            queries.append(row.replace(';',''))
        
        query = queries[0]
        numIter = 20

        ## User information
        user     = 'postgres'
        password = 'root'
        dataset = 'ssb_sf1'

        dt = str(datetime.datetime.now()).split('.')[0].replace('-','').replace(' ','').replace(':','')
        resfile = open('relatorios/spark/sf1Q11'+dt+'.csv','w')
        resfile.write('iter,start,end\n')

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

        ## Database information
        url = 'jdbc:postgresql://localhost:5432/' + dataset + '?user=' + user + '&password=' + password
        properties = {'driver': 'org.postgresql.Driver', 'password': password,'user': user}
        
        ## load tables
        df_customer = spark.read.jdbc(url=url, table='customer', properties=properties)
        df_date = spark.read.jdbc(url=url, table='date', properties=properties)
        df_lineorder = spark.read.jdbc(url=url, table='lineorder', properties=properties)
        df_part = spark.read.jdbc(url=url, table='part', properties=properties)
        df_supplier = spark.read.jdbc(url=url, table='supplier', properties=properties)

        ## temp tables
        df_customer.createOrReplaceTempView("customer")
        df_date.createOrReplaceTempView("date")
        df_lineorder.createOrReplaceTempView("lineorder")
        df_part.createOrReplaceTempView("part")
        df_supplier.createOrReplaceTempView("supplier")

        ## for numIter iterations
        for i in range(numIter):
            dtStart = datetime.datetime.now()
            res = spark.sql(query)
            dtEnd = datetime.datetime.now()
            resfile.write(str(i) + ',' + str(dtStart).split('.')[0] + ',' + str(dtEnd).split('.')[0] + '\n')
        spark.stop()        
        print("End Spark...")
    except KeyboardInterrupt:
        print("aborting...")
    except Exception as e:
        print(e)
    finally:
        exit()