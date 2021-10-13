import pymysql

connection = pymysql.connect(host='localhost', user = 'root', db='db')
cursor = connection.cursor()
# cursor.execute("DROP TABLE test")
# cursor.execute("CREATE TABLE test(name varchar(40), age int, dob DATETIME) ")
cursor.execute("SELECT * from test")
query=cursor.fetchall()
for q in query:
    print(q[0],q[1],q[2])
