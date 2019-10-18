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
        files = ['queries/q1.sql', 'queries/q2.sql', 'queries/q3.sql', 'queries/q4.sql']
        numIter = 20

        print("Start PostgreSQL...")

        ## executar o script para cada arquivo
        for sqlFile in files:
            ## abrir o arquivo .sql
            queries = loadSql(sqlFile)
            
            ## abrir conexão com o banco
            conn = psycopg2.connect('dbname=ssb_sf1 user=postgres password=root')
            cur = conn.cursor()

            ## para cada query
            q = 1
            for query in queries:
                ## cria o arquivo de relatório
                dt = str(datetime.datetime.now()).split('.')[0].replace('-','').replace(' ','').replace(':','')
                nameFile = 'relatorios/postgres/relatorio_' + sqlFile.split('/')[1].split('.')[0] + str(q) + '_' + dt +'.csv'
                finalFile = open(nameFile, "w")
                
                ## para numIter iterações, executa a query
                for i in range(0,numIter):
                    dtStart = datetime.datetime.now()
                    cur.execute(query)
                    cur.fetchall()
                    dtEnd = datetime.datetime.now()
                    finalFile.write(str(i) + ', ' + str(dtStart).split('.')[0] + ',' + str(dtEnd).split('.')[0] + '\n')
                
                ## fechar o arquivo
                finalFile.close()
                q += 1

            ## fechar conexão com o banco
            cur.close()
            conn.close()
        print("End PostgreSQL...")
    except KeyboardInterrupt:
        print("aborting...")
    except Exception as e:
        print(e)
    finally:
        exit()