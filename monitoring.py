import sys
import time
import psutil
import datetime

if __name__ == '__main__':
    try:
        print("CPU and Memory Monitor\nstarted...")
        
        sf = 'sf10'
        fw = 'spark'
        a = 3
        q = 2
        
        nameFile = 'relatorios/'+fw+'/'+sf+'/monitoring_q'+str(a+1)+str(q+1)+'.csv'
        
        finalFile = open(nameFile, "w")
        finalFile.write('datatime,cpuPercent,usedMB,memPercent\n')            
        
        while True:
            ## Time
            dataTime = datetime.datetime.now()

            ## CPU
            cpuPercent = psutil.cpu_percent(interval=0.1, percpu=False)

            ## MEMORY
            svmem = psutil.virtual_memory()
            dados = str(dataTime) + ',' + str(cpuPercent) + ',' + str(svmem.used) + ',' + str(svmem.percent) + '\n'

            finalFile.write(dados)

        print("ended...")
    except KeyboardInterrupt:
        print("aborting...")
    except:
        print("error...")
    finally:
        finalFile.close()