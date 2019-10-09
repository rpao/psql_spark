from sys import getsizeof
import memory_profiler
import psycopg2

@profile
def main():
    conn = psycopg2.connect('dbname=ssb_sf1 user=postgres password=root')
    cur = conn.cursor()
    print('Cur size: ', getsizeof(cur), 'bytes')
    query = 'Select * from customer'
    cur.execute(query)
    cur.fetchall()
    print('type of df: ', type(cur))
    print('Cur size: ', getsizeof(cur), 'bytes')
    cur.close()
    conn.close()

main()