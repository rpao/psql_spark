import datetime

import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession

if __name__ == '__main__':
    dataTime = datetime.datetime.now()
    print("Spark - Starting at " + str(dataTime) + "\n")

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

    #filterCustomer = rdd_c.filter( lambda x : x.c_nation.replace(' ','') == 'MOROCCO')       
    #filterCustomer = rdd_c.filter( lambda x : x.c_nation.replace(' ','') == 'AFRICA') 
    #filterCustomer = rdd_c.filter( lambda x : x.c_nation.replace(' ','') == 'PERU') 
    #filterCustomer = rdd_c.filter( lambda x : x.c_nation.replace(' ','') == 'CHINA') 
    #filterCustomer = rdd_c.filter( lambda x : x.c_nation.replace(' ','') == 'CANADA') 
    #filterCustomer = rdd_c.filter( lambda x : x.c_nation.replace(' ','') == 'FRANCE') 
    #filterCustomer = rdd_c.filter( lambda x : x.c_nation.replace(' ','') == 'INDIA') 
    
    rdd_c.unpersist()
    rdd_d.unpersist()
    rdd_l.unpersist()
    rdd_p.unpersist()
    rdd_s.unpersist()
    
    spark.stop()

    dataTime = datetime.datetime.now()
    print("Spark - Ending at " + str(dataTime) + "\n")

    exit()