import psycopg2
import datetime

def loadSql(file_name):
    file = open(file_name, "r")
    sql = []
    for line in file.readlines():
        sql.append(line.replace(';',''))
    return sql

if __name__ == '__main__':
    try:
        print('Starting PSQL...')
        dt = str(datetime.datetime.now()).split('.')[0].replace('-','').replace(' ','').replace(':','')
        nameFile = 'relatorios/postgres/relatorio_'+ dt +'.csv'
        
        finalFile = open(nameFile, "w")
        finalFile.write('PSQL - Starting at ' + str(datetime.datetime.now()) + '\n')

        numIter = 1
        conn = psycopg2.connect('dbname=ssb_sf1 user=postgres password=root')
        cur = conn.cursor()

        finalFile.write('PSQL - Quering at ' + str(datetime.datetime.now()) + '\n')
        for iter in range(0,numIter):
            for sqlFile in ['q1.sql', 'q2.sql', 'q3.sql', 'q4.sql']:
                queries = loadSql(sqlFile)
                for query in queries:
                    cur.execute(query)
                    cur.fetchall()    
        
        finalFile.write('PSQL - Ending at ' + str(datetime.datetime.now()) + '\n')
        cur.close()
        conn.close()
        print('Ending PSQL...')
    except KeyboardInterrupt:
        print("aborting...")
        finalFile.write('PSQL - aborting at ' + str(datetime.datetime.now()) + '\n')
    except Exception as e:
        print(e)
        finalFile.write('PSQL - error at ' + str(datetime.datetime.now()) + '\n')
    finally:
        finalFile.close()
        exit()