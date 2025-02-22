import time
import mysql.connector
from mysql.connector import Error
import threading
# n here stands for the number of queries
n = 10

execution_times = {}

def ExecuteQuery(connection,i,q):
    global execution_times
    f = open("ResultsFCFS5/result_"+str(i+1)+".txt","w+")
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

# Taking queries input
queries = []
for i in range(n):
    name = "Query/query_"+str(i+1)+".txt"
    f = open(name,"r+")
    for line in f.readlines():
        queries.append(line)
    f.close()

# Connecting with the database
try:
    connection1 = mysql.connector.connect(host='localhost',
                                         database='tpch',
                                         user='bordia',
                                         password='bordia98')
    
    connection2 = mysql.connector.connect(host='localhost',
                                         database='tpch',
                                         user='bordia',
                                         password='bordia98')

    connection3 = mysql.connector.connect(host='localhost',
                                         database='tpch',
                                         user='bordia',
                                         password='bordia98')

    connection4 = mysql.connector.connect(host='localhost',
                                         database='tpch',
                                         user='bordia',
                                         password='bordia98')

    connection5 = mysql.connector.connect(host='localhost',
                                         database='tpch',
                                         user='bordia',
                                         password='bordia98')
    if connection1.is_connected() and connection2.is_connected() and connection3.is_connected() and connection4.is_connected() and connection5.is_connected():
        print("Connection with database established")
        start = time.time()
        for i in range(0,len(queries),5):
            if i+4 < len(queries):
                t1 = threading.Thread(target=ExecuteQuery, args=(connection1,i,queries[i],))
                t2 = threading.Thread(target=ExecuteQuery, args=(connection2,i+1,queries[i+1],))
                t3 = threading.Thread(target=ExecuteQuery, args=(connection3,i+2,queries[i+2],))            
                t4 = threading.Thread(target=ExecuteQuery, args=(connection4,i+3,queries[i+3],))
                t5 = threading.Thread(target=ExecuteQuery, args=(connection5,i+4,queries[i+4],))
                t1.start()
                t2.start()
                t3.start()
                t4.start()
                t5.start()
                t1.join()
                t2.join()
                t3.join()
                t4.join()
                t5.join()
            else:
                thcount = 1
                threads = []
                while i < len(queries):
                    if thcount == 1:
                        conn = connection1
                    elif thcount == 2:
                        conn = connection2
                    elif thcount == 3:
                        conn = connection3
                    elif thcount == 4:
                        conn = connection4
                    else:
                        conn = connection5
                    th = threading.Thread(target = ExecuteQuery, args = (conn,i,queries[i]))
                    i += 1
                    thcount += 1
                    threads.append(th)

                for j in range(len(threads)):
                    threads[j].start()
                
                for j in range(len(threads)):
                    threads[j].join()

                break
        end = time.time()
        print("Total Execution time of FCFS approach(with 5 Threads) is : ", end - start)
except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if (connection1.is_connected()):
        connection1.close()
        print("MySQL connection1 is closed")

    if (connection2.is_connected()):
        connection2.close()
        print("MySQL connection2 is closed")

    if (connection3.is_connected()):
        connection3.close()
        print("MySQL connection3 is closed")
    
    if (connection4.is_connected()):
        connection4.close()
        print("MySQL connection4 is closed")
    
    if (connection5.is_connected()):
        connection5.close()
        print("MySQL connection5 is closed")

for i in range(n):
    print("Execution Time of Query" , i+1, " = ",execution_times[i+1])
