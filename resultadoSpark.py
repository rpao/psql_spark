import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession

if __name__ == '__main__':
    try:
        sqlFile = open('queries/qs.sql', 'r')
        queries = []
        for row in sqlFile.readlines():
            queries.append(row.replace(';',''))
        sqlFile.close()

        ## abrir conexão com o banco
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
        
        ## carregar tabelas
        df_customer = spark.read.jdbc(url=url, table='customer', properties=properties)
        df_date = spark.read.jdbc(url=url, table='date', properties=properties)
        df_lineorder = spark.read.jdbc(url=url, table='lineorder', properties=properties)
        df_part = spark.read.jdbc(url=url, table='part', properties=properties)
        df_supplier = spark.read.jdbc(url=url, table='supplier', properties=properties)

        ## criar tempViews
        df_customer.createOrReplaceTempView("customer")
        df_date.createOrReplaceTempView("date")
        df_lineorder.createOrReplaceTempView("lineorder")
        df_part.createOrReplaceTempView("part")
        df_supplier.createOrReplaceTempView("supplier")

        ## para cada query, guarda o resultado
        q = 1
        res = []
        for query in queries:
            print("Executando Q"+str(q))
            nf = 'relatorios/resultadoQuerySpark/res_SF10_Q'+str(q)+'.csv'
            r = spark.sql(query).collect()
            resfile = open(nf, "w")
            for row in r:
                resfile.write(str(row))
            resfile.close()
            q += 1
        spark.stop()
    except Exception as e:
        print(e)

