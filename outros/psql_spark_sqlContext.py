import memory_profiler
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext

def loadSqlContext():
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)
    return sqlContext

@profile
def execQuery(query):
    df = sqlContext.read.format('jdbc').options(driver = "org.postgresql.Driver", 
            url="{0}/{1}?useTimezone=true&serverTimezone=UTC&user={2}&password={3}".format(url, database,user,password), 
            dbtable=query).load()
    return df

## READ CONFIG
configs = open("config.txt", "r")
appName = configs.readline().replace('\n','').replace('\0','').split('=')[1]
connector = configs.readline().replace('\n','').replace('\0','').split('=')[1]
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

sqlContext = loadSqlContext()
query = "select * from supplier"
df = execQuery(query)

if (df != None):
    print(df.printSchema())

exit()