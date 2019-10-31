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
        sqlFile = open('queries/qs.sql', 'r')
        queries = []
        for line in sqlFile.readlines():
            queries.append(line.replace(';',''))
        
        ## número de iterações
        numIter = 20
        query = queries[0]

        print("Start PostgreSQL...")

        ## cria o arquivo de relatório
        dt = str(datetime.datetime.now()).split('.')[0].replace('-','').replace(' ','').replace(':','')
        nameFile = 'relatorios/postgres/SF1Q11' + dt +'.csv'
        finalFile = open(nameFile, "w")
        finalFile.write('iter,ini,end,rowCount')

        ## abrir conexão com o banco
        conn = psycopg2.connect('dbname=ssb_sf1 user=postgres password=root')
        cur = conn.cursor()

        for i in range(0,numIter):
            dtStart = datetime.datetime.now()
            cur.execute(query)
            cur.fetchall()
            dtEnd = datetime.datetime.now()
            finalFile.write(str(i) + ', ' + str(dtStart).split('.')[0] + ',' + str(dtEnd).split('.')[0] + ',' + cur.rowcount + '\n')
            
        ## fechar o arquivo
        finalFile.close()

        nameFile = 'relatorios/postgres/resSF1Q11FetchAll_' + dt +'.csv'
        finalFile = open(nameFile, "w")

        cur.execute(query)
        rows = cur.fetchall()
        for line in rows:
            finalFile.write(line + '\n')
        finalFile.close()
            
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