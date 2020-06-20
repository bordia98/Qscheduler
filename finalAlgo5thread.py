import time
import mysql.connector
from mysql.connector import Error
import threading
# n here stands for the number of queries
n = 10

execution_times = {}

def findsim(a,b,n):
    tot = 0
    for i in range(n):
        if a[i] == b[i] and a[i] == 1 and b[i] == 1:
            tot += 1
    return tot
 
def ExecuteQuery(connection,i,q):
    global execution_times
    f = open("ResultsNewAlgo5/result_"+str(i+1)+".txt","w+")
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


tables = ["customer" ,"lineitem" ,"nation" ,"orders" ,"part" , "partsupp" ,"region"  , "supplier" ]
queries = []
mat = [ [ 0 for i in range(len(tables) + 2)] for j in range(n)]

for i in range(n):
    name = "Query/query_"+str(i+1)+".txt"
    f = open(name,"r+")
    for line in f.readlines():
        queries.append(line)
    f.close()

count = 0
for query in queries:
    
    dict = {}
    val = list(query.split(" "))
    for i in val:
        i = i.strip()
        newval = list(i.split(","))
        for j in newval:
            j = j.strip()
            if j not in dict:
                dict[j] = 1
            else:
                dict[j] += 1
    
    totalcount = 0
    for i in range(len(tables)):
        if tables[i] in dict:
            mat[count][i] = 1
            totalcount += 1
    mat[count][len(tables)] = totalcount
    mat[count][len(tables)+1] = count
    count += 1

mat = sorted(mat, key=lambda  item: item[len(tables)], reverse= True)

done = [0]*n
order = []
while True:
    maxsim = 0
    one,two = -1,-1
    for i in range(n):
        if done[i] == 1:
            pass
        else:
            for j in range(n):
                if done[j] == 1:
                    pass
                else:
                    if i!=j:
                        val = findsim(mat[i],mat[j],len(tables))
                        if val > maxsim:
                            one = i
                            two = j
                            maxsim = val
    done[one] = 1
    done[two] = 1
    order.append(one)
    order.append(two)
    if len(order) == n:
        break

print(order)

print("Matrix of the queries is as follows")
for i in range(n):
    print(mat[order[i]])

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
                t1 = threading.Thread(target=ExecuteQuery, args=(connection1,mat[order[i]][len(tables)+1],queries[mat[order[i]][len(tables)+1]],))
                t2 = threading.Thread(target=ExecuteQuery, args=(connection2,mat[order[i+1]][len(tables)+1],queries[mat[order[i+1]][len(tables)+1]],))
                t3 = threading.Thread(target=ExecuteQuery, args=(connection3,mat[order[i+2]][len(tables)+1],queries[mat[order[i+2]][len(tables)+1]],))
                t4 = threading.Thread(target=ExecuteQuery, args=(connection4,mat[order[i+3]][len(tables)+1],queries[mat[order[i+3]][len(tables)+1]],))
                t5 = threading.Thread(target=ExecuteQuery, args=(connection5,mat[order[i+4]][len(tables)+1],queries[mat[order[i+4]][len(tables)+1]],))
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
                    else:
                        conn = connection2
                    th = threading.Thread(target = ExecuteQuery, args = (conn,mat[order[i]][len(tables)+1],queries[mat[order[i]][len(tables)+1]]))
                    i += 1
                    thcount += 1
                    threads.append(th)

                for j in range(len(threads)):
                    threads[j].start()
                
                for j in range(len(threads)):
                    threads[j].join()
                break
        end = time.time()
        print("Total Execution time of New approach(with 5 Threads) is : ", end - start)
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
