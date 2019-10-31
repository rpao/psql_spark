import time
import psycopg2
import datetime

if __name__ == '__main__':
    try:
        t = 60
        a = 3
        q = 2
        sf = 'sf10'
        
        time.sleep(3*t) ## esperar dois minutos antes de iniciar

        print("Start Psql...")

        ## User information
        user     = 'postgres'
        password = 'root'
        dataset  = 'ssb_'+sf

        confConnection = 'dbname='+dataset+' user=' + user + ' password=' + password
        
        numIter = 20
        arquivos = ['queries/q1.sql', 'queries/q2.sql', 'queries/q3.sql', 'queries/q4.sql']

        sqlfile = open(arquivos[a],'r')
        queries = []
        for row in sqlfile.readlines():
            queries.append(row.replace(';',''))
        
        query = queries[q]

        ## abrir conexão com o banco
        conn = psycopg2.connect(confConnection)
        cur = conn.cursor()

        resfile = open('relatorios/psql/'+sf+'/q'+str(a+1)+str(q+1)+'_iteracoes.csv','w')
        resfile.write('iter,start,end\n')

        for i in range(numIter):
            dtStart = datetime.datetime.now()
            cur.execute(query)
            ## como não há collect no spark, psql não usa fetch
            dtEnd = datetime.datetime.now()
            resfile.write(str(i) + ', ' + str(dtStart) + ',' + str(dtEnd) + '\n')

        resfile.close()

        ## fechar conexão com o banco
        cur.close()
        conn.close()

        time.sleep(t) ## esperar dois minutos antes de iniciar
        
        print("End PostgreSQL...")
    except KeyboardInterrupt:
        print("aborting...")
    except Exception as e:
        print(e)
    finally:
        exit()