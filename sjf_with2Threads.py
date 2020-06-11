import time
import mysql.connector
from mysql.connector import Error
import threading


execution_times = {}

def ExecuteQuery(connection,i,q):
    global execution_times
    f = open("ResultsSJF2/result_"+str(i+1)+".txt","w+")
    start = time.time()
    cursor = connection.cursor()
    cursor.execute(q);
    records = cursor.fetchall()
    end = time.time()

    for record in records:
        f.write(str(record))
        f.write("\n")
    execution_times[i+1] = end - start
    f.write("Execution Time: " + str(end-start))
    f.close()
    cursor.close()

n = 10

# Taking queries input
queries = []
for i in range(n):
    name = "Query/query_"+str(i+1)+".txt"
    f = open(name,"r+")
    for line in f.readlines():
        queries.append(line)
    f.close()
timing = []

f = open("AverageTime.txt","r+")
count = 0
for line in f.readlines():
    val = list(line.split("="))
    tim = float(val[-1].strip())
    timing.append([count,tim])
    count += 1

timing = sorted(timing, key=lambda item: item[1])

# Connecting with the database
try:
    connection1 = mysql.connector.connect(host='localhost',
                                         database='tpch',
                                         user='bordia',
                                         password='')
    
    connection2 = mysql.connector.connect(host='localhost',
                                         database='tpch',
                                         user='bordia',
                                         password='')
    
    if connection1.is_connected() and connection2.is_connected():
        print("Connection established with database")
        start = time.time()
        for i in range(0,len(queries),2):
            if i+1 < len(queries):
                t1 = threading.Thread(target=ExecuteQuery, args=(connection1,timing[i][0],queries[timing[i][0]],))
                t2 = threading.Thread(target=ExecuteQuery, args=(connection2,timing[i+1][0],queries[timing[i+1][0]],))
                t1.start()
                t2.start()
                t1.join()
                t2.join()
            else:
                thcount = 1
                threads = []
                while i < len(queries):
                    if thcount == 1:
                        conn = connection1
                    else:
                        conn = connection2
                    th = threading.Thread(target = ExecuteQuery, args = (conn,timing[i][0],queries[timing[i][0]]))
                    i += 1
                    thcount += 1
                    threads.append(th)

                for j in range(len(threads)):
                    threads[j].start()
                
                for j in range(len(threads)):
                    threads[j].join()
                break
        end = time.time()
        print("Total Execution time of SJF approach(With 2 Threads) is = ",end - start)
except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if (connection1.is_connected()):
        connection1.close()
        print("MySQL connection1 is closed")

    if (connection2.is_connected()):
        connection2.close()
        print("MySQL connection2 is closed")

for i in range(n):
    print("Execution Time of Query" , i+1, " = ",execution_times[i+1])
    