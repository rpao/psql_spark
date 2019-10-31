import time
import datetime

import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession

if __name__ == '__main__':
    try:
        t = 60
        sf = 'sf10'
        a = 3
        q = 2

        time.sleep(3*t) ## esperar dois minutos antes de iniciar

        print("Start Spark...")

        ## User information
        user     = 'postgres'
        password = 'root'
        dataset  = 'ssb_'+sf
        
        numIter = 20
        arquivos = ['queries/q1.sql', 'queries/q2.sql', 'queries/q3.sql', 'queries/q4.sql']
        
        sqlfile = open(arquivos[a],'r')
        queries = []
        for row in sqlfile.readlines():
            queries.append(row.replace(';',''))

        query = queries[q]
        
        tempFile = open('relatorios/spark/'+sf+'/q' + str(a+1) + str(q+1) + '_tempView.csv','w')
        tempFile.write('op,start,end\n')

        resfile = open('relatorios/spark/'+sf+'/q' + str(a+1) + str(q+1) + '_iteracoes.csv','w')
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
        dtStartTemp = datetime.datetime.now()
        df_customer = spark.read.jdbc(url=url, table='customer', properties=properties)
        df_date = spark.read.jdbc(url=url, table='date', properties=properties)
        df_lineorder = spark.read.jdbc(url=url, table='lineorder', properties=properties)
        df_part = spark.read.jdbc(url=url, table='part', properties=properties)
        df_supplier = spark.read.jdbc(url=url, table='supplier', properties=properties)
        dtEndTemp = datetime.datetime.now()
        tempFile.write('LOAD,' + str(dtStartTemp) + ',' + str(dtEndTemp) + '\n')               

        ## temp tables
        dtStartTemp = datetime.datetime.now()
        df_customer.createOrReplaceTempView("customer")
        df_date.createOrReplaceTempView("date")
        df_lineorder.createOrReplaceTempView("lineorder")
        df_part.createOrReplaceTempView("part")
        df_supplier.createOrReplaceTempView("supplier")
        dtEndTemp = datetime.datetime.now()
        tempFile.write('CREATE_VIEW,' + str(dtStartTemp) + ',' + str(dtEndTemp) + '\n')

        ## for numIter iterations
        for i in range(numIter):
            dtStart = datetime.datetime.now()
            res = spark.sql(query)
            ## como não há collect, psql não usa fetch
            dtEnd = datetime.datetime.now()
            resfile.write(str(i) + ',' + str(dtStart) + ',' + str(dtEnd) + '\n')

        ## fechar sessão spark
        tempFile.close()
        resfile.close()
        spark.stop()
        q += 1
        time.sleep(t)
        print("End Spark...")
    except KeyboardInterrupt:
        print("aborting...")
    except Exception as e:
        print(e)
    finally:
        exit()