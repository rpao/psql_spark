# psql_spark

Executar com PostgreSQL connector:
	1. Comando para executar o pyspark no jupyter notebook:	
		pyspark --jars "/connectors/postgresql-42.2.8.jar"

	2. Comando para executar no cmd (com memory_profiler):
		python -m memory_profiler psql_spark.py
		python -m mprof run psql_spark.py