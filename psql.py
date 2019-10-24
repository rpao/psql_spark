import psycopg2
import datetime

if __name__ == '__main__':
    try:
        print("Start Psql...")

        ## arquivos: q1.sql, q2.sql, q3.sql, q4.sql; 
        sqlfile = open('queries/q1.sql','r')
        queries = []
        for row in sqlfile.readlines():
            queries.append(row.replace(';',''))
        
        query = queries[0]
        numIter = 20

        dt = str(datetime.datetime.now()).split('.')[0].replace('-','').replace(' ','').replace(':','')
        resfile = open('relatorios/postgres/sf1Q11'+dt+'.csv','w')
        resfile.write('iter,start,end\n')
            
        ## abrir conexão com o banco
        conn = psycopg2.connect('dbname=ssb_sf1 user=postgres password=root')
        cur = conn.cursor()

        for i in range(numIter):
            dtStart = datetime.datetime.now()
            cur.execute(query)
            cur.fetchall()
            dtEnd = datetime.datetime.now()
            resfile.write(str(i) + ', ' + str(dtStart).split('.')[0] + ',' + str(dtEnd).split('.')[0] + '\n')

        resfile.close()

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