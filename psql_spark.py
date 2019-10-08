import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession

def loadTable(table, session):
    ## User information
    user     = 'postgres'
    password = 'root'
    dataset = 'ssb_sf1'

    ## Database information
    url = 'jdbc:postgresql://localhost:5432/' + dataset + '?user=' + user + '&password=' + password
    properties ={'driver': 'org.postgresql.Driver', 'password': password,'user': user}

    try:
        time.sleep(1)
        df = spark.read.jdbc(url=url, table=table, properties=properties)
        time.sleep(1)
        if (df == None):
            print ('Table ' + table + ' is empty!')
            return None
        
        ## table info
        print('Table ' + table + ' infos:')
        print('Number of lines: ', df.count(),'\nSchema:')
        print(df.printSchema())
        df.show(5)

        ## convert from RDD
        time.sleep(1)
        rdd = df.rdd
        print('loadTable('+table+'): Success!')
        time.sleep(1)

        return rdd
    except:
        print('loadTable('+table+'): Error!')
        return None

def main():
    ## create spark session
    spark = SparkSession \
        .builder \
        .appName('Postgres Spark') \
        .config('spark.jars', 'connectors/postgresql-42.2.8.jar') \
        .getOrCreate()

    rdd_c = loadTable('customer', spark)
    if (rdd_c == None):
        print ('Customer is empty!')
        return None

    rdd_s = loadTable('supplier', spark)
    if (rdd_s == None):
        print ('Supplier is empty!')
        return None

    rdd_c.persist(pyspark.StorageLevel.MEMORY_ONLY)
    print('Customer - Total Rows: ', rdd_c.count())

    rdd_s.persist(pyspark.StorageLevel.MEMORY_ONLY)
    print('Total Rows: ', rdd_s.count())

    filter_rdd = rdd_c.filter( lambda x : x.c_nation.replace(' ','') == 'MOROCCO')
    print('Customer filter c_nation == MOROCCO - Total Rows: ', filter_rdd.count())

    rdd_c.unpersist()
    rdd_s.unpersist()

main()

exit(0)