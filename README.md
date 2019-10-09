# psql_spark

Executar com PostgreSQL connector:
	1. Comando para executar o pyspark no jupyter notebook:	
		pyspark --jars "/connectors/postgresql-42.2.8.jar"

	2. Comando para executar no cmd:
		python monitoring.py -- arquivo de monitoramento de CPU e memória RAM
		python spark.py		 -- arquivo que executa o spark
		python psql.py  	 -- arquivo que executa o python com postgreSql