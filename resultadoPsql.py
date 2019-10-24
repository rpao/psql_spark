import psycopg2

if __name__ == '__main__':
    sqlFile = open('queries/qs.sql', 'r')
    queries = []
    for row in sqlFile.readlines():
        queries.append(row.replace(';',''))
    sqlFile.close()

    ## abrir conexão com o banco
    conn = psycopg2.connect('dbname=ssb_sf10 user=postgres password=root')
    cur = conn.cursor()

    ## para cada query, guarda o resultado
    q = 1
    for query in queries:
        print("Executando Q"+str(q))
        resfile = open('relatorios/resultadoQueryPSQL/resSF10Q'+str(q)+'.csv', "w")

        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            resfile.write(str(row)+'\n')
        resfile.close()
        q += 1
            
    ## fechar conexão com o banco
    cur.close()
    conn.close()