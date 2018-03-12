import pyodbc

# Specifying the ODBC driver, server name, database, etc. directly
# cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=localhost;DATABASE=testdb;UID=me;PWD=pass')
cnxn = pyodbc.connect('DRIVER={MySQL};SERVER=localhost;DATABASE=x;UID=root;PWD=root')

# Using a DSN, but providing a password as well
# cnxn = pyodbc.connect('DSN=test;PWD=password')

# Create a cursor from the connection
cursor = cnxn.cursor()

# This is just an example that works for PostgreSQL and MySQL, with Python 2.7.
cnxn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
cnxn.setencoding(encoding='utf-8')

cursor.execute("select user_id, user_name from users")
row = cursor.fetchone()
if row:
    print(row)

cursor.execute("select user_id, user_name from users")
row = cursor.fetchone()
print('name:', row[1])          # access by column index (zero-based)
print('name:', row.user_name)   # access by name
