import time
import psycopg2
import datetime

if __name__ == '__main__':
    try:
        timeSleep = 300 # cinco minutos
        time.sleep(timeSleep)

        ## configuração do banco
        user     = 'postgres'
        password = 'root'
        dataset = 'ssb_sf10'

        confConnection = 'dbname='+dataset+' user=' + user + ' password=' + password

        print("Start Psql...")
        
        a = 1
        numIter = 20
        arquivos = ['queries/q1.sql', 'queries/q2.sql', 'queries/q3.sql', 'queries/q4.sql'] 
        for arquivo in arquivos:
            sqlfile = open(arquivo,'r')
            queries = []
            for row in sqlfile.readlines():
                queries.append(row.replace(';',''))
            
            q = 1
            for query in queries:
                ## abrir conexão com o banco
                conn = psycopg2.connect(confConnection)
                cur = conn.cursor()

                dt = str(datetime.datetime.now()).split('.')[0].replace('-','').replace(' ','').replace(':','')
                resfile = open('relatorios/postgres/'+dataset+'_q'+str(a)+str(q)+'_'+dt+'.csv','w')
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
                
                q += 1
                time.sleep(timeSleep)
            a += 1
        print("End PostgreSQL...")
    except KeyboardInterrupt:
        print("aborting...")
    except Exception as e:
        print(e)
    finally:
        exit()