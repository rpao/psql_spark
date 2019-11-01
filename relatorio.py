import datetime
import statistics

if __name__ == '__main__':

    fws = ['psql', 'spark']

    sfs = ['sf1', 'sf10']
    
    queries = ['q11','q12','q13',
                'q21','q22','q23',
                'q31','q32','q33','q34',
                'q41','q42','q43',]

    for fw in fws:
        for sf in sfs:                
            for q in queries:
                if (fw == fws[1]) :
                    tempOrigem = 'relatorios/' + fw + '/' + sf + '/' + q + '_tempView.csv'
                    loadRelatorio = 'relatorios/dados/' + fw + '_' + sf + '/LoadTable.csv'
                    createRelatorio = 'relatorios/dados/' + fw + '_' + sf + '/CreateTmpView.csv'
                    
                    origin = open(tempOrigem, 'r')
                    ## retirar o cabeçalho
                    origin.readline()

                    dados = []                
                    for line in origin.readlines():
                        dados.append(line.replace(', ',',').replace('\n','').split(','))
                    origin.close()

                    micro = 1000000
                    formatDate = '%Y-%m-%d %H:%M:%S.%f'

                    loadFile = open(loadRelatorio,'a')
                    createFile = open(createRelatorio,'a')

                    for i in range(len(dados)):
                        inicio =  datetime.datetime.strptime(dados[i][1], formatDate) 
                        fim = datetime.datetime.strptime(dados[i][2], formatDate)
                        duracao = datetime.datetime.__sub__(fim, inicio)
                        if (dados[i][0] == 'CREATE_VIEW'):
                            createFile.write(q + ',' + str(duracao.seconds + duracao.microseconds/micro)+'\n')
                        elif (dados[i][0] == 'LOAD'):
                            loadFile.write(q + ',' + str(duracao.seconds + duracao.microseconds/micro)+'\n')
                        
                    loadFile.close()
                    createFile.close()

                origem = 'relatorios/' + fw + '/' + sf + '/' + q + '_iteracoes.csv'
                relatorio = 'relatorios/dados/' + fw + '_' + sf + '/' + q + '.csv'

                print (origem)

                origin = open(origem, 'r')
                ## retirar o cabeçalho
                origin.readline()

                dados = []                
                for line in origin.readlines():
                    dados.append(line.replace(', ',',').replace('\n','').split(','))
                origin.close()

                micro = 1000000
                formatDate = '%Y-%m-%d %H:%M:%S.%f'

                tempo = []
                for i in range(len(dados)):
                    inicio =  datetime.datetime.strptime(dados[i][1], formatDate) 
                    fim = datetime.datetime.strptime(dados[i][2], formatDate)
                    duracao = datetime.datetime.__sub__(fim, inicio)
                    tempo.append(duracao.seconds + duracao.microseconds/micro)
                
                media = statistics.mean(tempo)
                desvioPadrao = statistics.stdev(tempo) 

                formated = open(relatorio,'w')
                formated.write("DuracaoIteracao\n")
                for t in tempo:
                    formated.write(str(t) + '\n')
                formated.write('\nMEDIA: ' + str(media) + '\nDESVIO_PADRAO: ' + str(desvioPadrao))
                formated.close()

    for sf in sfs:
        arquivos = ['relatorios/dados/spark_' + sf + '/LoadTable.csv',
                    'relatorios/dados/spark_' + sf + '/CreateTmpView.csv']
        for loadRelatorio in arquivos:
            loadFile = open(loadRelatorio, 'r')
            loadDados = []                
            for line in loadFile.readlines():
                try:
                    t = line.replace(', ',',').replace('\n','').split(',')[1]
                    print (t)
                    loadDados.append(float(t))
                except:
                    print(line)
            loadFile.close()
            
            media = statistics.mean(loadDados)
            desvioPadrao = statistics.stdev(loadDados) 

            loadFile = open(loadRelatorio,'a')
            loadFile.write('MEDIA: ' + str(media) + '\nDESVIO PADRAO: ' + str(desvioPadrao))
            loadFile.close()

    print("ended ...")