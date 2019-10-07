import memory_profiler
import findspark
findspark.init()

from pyspark.sql import SparkSession

def loadSesson(appName, connector):
    spark = SparkSession \
    .builder \
    .appName(appName) \
    .config("spark.jars", connector) \
    .getOrCreate()
    return spark

@profile
def loadTable(table):
    df = None
    try:
"""         df = spark.read \
            .format("jdbc") \
            .option("url",  url + database) \
            .option("dbtable", dbtable) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", driver) \
            .load() """
        properties = {'driver':driver, 'password':password, 'user':user}
        df = spark.read.jdbc(url=url, table=table, properties=properties)
        print("loadTable(): Success!")
    except:
        print("loadTable(): Error! [PostgresSql (databaseURL = " + url + database +
         " user = " + user +
         "password = " + password +
         "dbtable = " + dbtable + ")]")
    finally:
        return df

## READ CONFIG
configs = open("config.txt", "r")
appName = configs.readline().replace('\n','').replace('\0','').split('=')[1]
connector = configs.readline().replace('\n','').replace('\0','').split('=')[1]
driver = configs.readline().replace('\n','').replace('\0','').split('=')[1]
url = configs.readline().replace('\n','').replace('\0','').split('=')[1]
user = configs.readline().replace('\n','').replace('\0','').split('=')[1]
password = configs.readline().replace('\n','').replace('\0','').split('=')[1]
database = configs.readline().replace('\n','').replace('\0','').split('=')[1]
configs.close()

print ('Configs:' + 
       '\n--------'
        '\nappName: ' + appName + 
        '\nconnector: ' + connector +
        '\nurlDB: ' + url +
        '\nuser: ' + user +
        '\npassword: ' + password +
        '\ndatabase: ' + database)
        
spark = loadSesson(appName, connector)
lineorder_df = loadTable("lineorder")

if (lineorder_df != None):
    print(lineorder_df.printSchema())

exit()