import sys
import time
import psutil
#import logging
import datetime
import threading

import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession

#logging.basicConfig(level=#logging.DEBUG,
#                    format='(%(threadName)-9s) %(message)s',)

def loadTable(table, session):
    ## User information
    user     = 'postgres'
    password = 'root'
    dataset = 'ssb_sf1'

    ## Database information
    url = 'jdbc:postgresql://localhost:5432/' + dataset + '?user=' + user + '&password=' + password
    properties ={'driver': 'org.postgresql.Driver', 'password': password,'user': user}

    try:
        df = spark.read.jdbc(url=url, table=table, properties=properties)
        if (df == None):
            return None

        ## convert from RDD
        rdd = df.rdd
        return rdd

    except:
        #logging.debug('loadTable('+table+'): Error!\n')
        return None

def monitor(e):
    #logging.debug("CPU and Memory Monitor - Making thread\n")
    print("CPU and Memory Monitor - Making thread\n")
    
    startTime = datetime.datetime.now()
    relatorio = ['datatime,cpuPercent,usedMB,memPercent', "\n"]            
    
    while not e.isSet():
        ## Time
        dataTime = datetime.datetime.now()

        ## CPU
        cpuPercent = psutil.cpu_percent(interval=0.1, percpu=False)

        ## MEMORY
        svmem = psutil.virtual_memory()
        dados = str(dataTime) + ',' + str(cpuPercent) + ',' + str(svmem.used) + ',' + str(svmem.percent) + '\n'

        relatorio.append(dados)

    #logging.debug("CPU and Memory Monitor - Ending thread\n")
    print("CPU and Memory Monitor - Ending thread\n")

    nameFile = 'relatorio'+str(startTime)
    nameFile = nameFile.replace(' ','_').replace('.','_').replace(':','_') + '.txt'

    finalFile = open(nameFile, "w")
    finalFile.writelines( relatorio )
    finalFile.close()

def stopMonitor(e):
    dataTime = datetime.datetime.now()
    print("Stop Monitor - Making thread at " + str(dataTime) + "\n")

    time.sleep(120)

    dataTime = datetime.datetime.now()
    print("Stop Monitor - Ending thread at " + str(dataTime) + "\n")

    e.set()

def spark(e):
    time.sleep(60)
    
    dataTime = datetime.datetime.now()
    #logging.debug("Spark - Making thread at " + str(dataTime) + "\n")
    print("Spark - Making thread at " + str(dataTime) + "\n")

    ## create spark session
    spark = SparkSession \
        .builder \
        .appName('Postgres Spark') \
        .config('spark.jars', 'connectors/postgresql-42.2.8.jar') \
        .getOrCreate()

    ## User information
    user     = 'postgres'
    password = 'root'
    dataset = 'ssb_sf10'

    ## Database information
    url = 'jdbc:postgresql://localhost:5432/' + dataset + '?user=' + user + '&password=' + password
    properties = {'driver': 'org.postgresql.Driver', 'password': password,'user': user}
    
    rdd_c = spark.read.jdbc(url=url, table='customer', properties=properties).rdd
    rdd_d = spark.read.jdbc(url=url, table='date', properties=properties).rdd
    rdd_l = spark.read.jdbc(url=url, table='lineorder', properties=properties).rdd
    rdd_p = spark.read.jdbc(url=url, table='part', properties=properties).rdd
    rdd_s = spark.read.jdbc(url=url, table='supplier', properties=properties).rdd

    rdd_c.persist(pyspark.StorageLevel.MEMORY_ONLY)        
    rdd_d.persist(pyspark.StorageLevel.MEMORY_ONLY)
    rdd_l.persist(pyspark.StorageLevel.MEMORY_ONLY)        
    rdd_p.persist(pyspark.StorageLevel.MEMORY_ONLY)
    rdd_s.persist(pyspark.StorageLevel.MEMORY_ONLY)

    # if (rdd_c != None and rdd_s != None):
    #     # filterCustomer = rdd_c.filter( lambda x : x.c_nation.replace(' ','') == 'MOROCCO')       
    #     # filterCustomer = rdd_c.filter( lambda x : x.c_nation.replace(' ','') == 'AFRICA') 
    #     # filterCustomer = rdd_c.filter( lambda x : x.c_nation.replace(' ','') == 'PERU') 
    #     # filterCustomer = rdd_c.filter( lambda x : x.c_nation.replace(' ','') == 'CHINA') 
    #     # filterCustomer = rdd_c.filter( lambda x : x.c_nation.replace(' ','') == 'CANADA') 
    #     # filterCustomer = rdd_c.filter( lambda x : x.c_nation.replace(' ','') == 'FRANCE') 
    #     # filterCustomer = rdd_c.filter( lambda x : x.c_nation.replace(' ','') == 'INDIA') 
    # else:
    #     #logging.debug('Not Load Customer or Supplier Table!')
    #     print('Not Load Customer or Supplier Table!')

    rdd_c.unpersist()
    rdd_d.unpersist()
    rdd_l.unpersist()
    rdd_p.unpersist()
    rdd_s.unpersist()
    
    spark.stop()
    spark.clearCache()

    dataTime = datetime.datetime.now()
    #logging.debug("Spark - Ending thread at " + str(dataTime) + "\n")
    print("Spark - Ending thread at " + str(dataTime) + "\n")

    ## espera o monitor executar por mais 1 minuto antes de finalizar o monitor
    time.sleep(60)
    e.set()

if __name__ == '__main__':
    QTD_ITERATION = 20

    ## Create monitor memory and cpu usage pattern
    for (i=0; i < QTD_ITERATION; i++):
        e = threading.Event()

        monitorTh = threading.Thread(name='blocking', 
                        target=monitor,
                        args=(e,))
        monitorTh.start()

        stopMonitorTh = threading.Thread(name='non-blocking', 
                        target=stopMonitor, 
                        args=(e,))
        stopMonitorTh.start()
    
    ## Create spark memory and cpu usage pattern
    for (i=0; i < QTD_ITERATION; i++):
        e = threading.Event()

        monitorTh = threading.Thread(name='blocking', 
                        target=monitor,
                        args=(e,))
        monitorTh.start()

        sparkTh = threading.Thread(name='non-blocking', 
                        target=spark, 
                        args=(e,))
        sparkTh.start()