import time
import datetime

import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession

if __name__ == '__main__':
    try:
        timeSleep = 300 # cinco minutos
        time.sleep(timeSleep)

        ## User information
        user     = 'postgres'
        password = 'root'
        dataset = 'ssb_sf10'

        print("Start Spark...")

        a = 1
        numIter = 20
        arquivos = ['queries/q1.sql', 'queries/q2.sql', 'queries/q3.sql', 'queries/q4.sql']; 
        for arquivo in arquivos:
            sqlfile = open(arquivo,'r')
            queries = []
            for row in sqlfile.readlines():
                queries.append(row.replace(';',''))

            q = 1
            tempFile = open('relatorios/spark/'+dataset+'_tempView'+str(a)+'.csv','w')
            tempFile.write('op,query,start,end\n')
            for query in queries:
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
                tempFile.write('LOAD,'+str(q) + ',' + str(dtStartTemp) + ',' + str(dtEndTemp) + '\n')               

                ## temp tables
                dtStartTemp = datetime.datetime.now()
                df_customer.createOrReplaceTempView("customer")
                df_date.createOrReplaceTempView("date")
                df_lineorder.createOrReplaceTempView("lineorder")
                df_part.createOrReplaceTempView("part")
                df_supplier.createOrReplaceTempView("supplier")
                dtEndTemp = datetime.datetime.now()
                tempFile.write('CREATE_VIEW,' + str(q) + ',' + str(dtStartTemp) + ',' + str(dtEndTemp) + '\n')

                dt = str(datetime.datetime.now()).split('.')[0].replace('-','').replace(' ','').replace(':','')
                resfile = open('relatorios/spark/'+dataset+'_q'+str(a)+str(q)+'_'+dt+'.csv','w')
                resfile.write('iter,start,end\n')
            
                ## for numIter iterations
                for i in range(numIter):
                    dtStart = datetime.datetime.now()
                    res = spark.sql(query)
                    ## como não há collect, psql não usa fetch
                    dtEnd = datetime.datetime.now()
                    resfile.write(str(i) + ',' + str(dtStart) + ',' + str(dtEnd) + '\n')
                    
                q += 1

                ## fechar sessão spark
                spark.stop()

                time.sleep(timeSleep)

            a += 1

        print("End Spark...")
    except KeyboardInterrupt:
        print("aborting...")
    except Exception as e:
        print(e)
    finally:
        exit()