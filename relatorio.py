import datetime

if __name__ == '__main__':
    arquivo = 'relatorios/spark/sf10/monitoring_q3'
    arquivo2 = 'relatorios/spark/sf10/monitoring_f_q3'
    query = ['1','2','3','4']

    for q in query:
        origin = open(arquivo + q + '.csv', 'r')
        ## retirar o cabeçalho
        origin.readline()

        dados = []
        dadosFormatados = []   

        for line in origin.readlines():
            dados.append(line.replace('\n','').split(','))
        origin.close()

        tempo = 0
        formatDate = '%Y-%m-%d %H:%M:%S.%f'
        dtAnterior = datetime.datetime.strptime(dados[0][0], formatDate)
        dadosFormatados.append('microseconds,cpuPercent,usedMB,memPercent\n')
        for i in range(len(dados)):
            ## intervalo de tempo
            difTempo = dtAnterior - datetime.datetime.strptime(dados[i][0], formatDate)
            tempo += difTempo.microseconds
            dtAnterior = datetime.datetime.strptime(dados[i][0], formatDate)
            dadosFormatados.append(str(tempo)+','+dados[i][1]+','+dados[i][2]+','+dados[i][3]+'\n')
        
        formated = open(arquivo2 + q + '.csv','w')
        formated.writelines(dadosFormatados)

        print("ended ...")