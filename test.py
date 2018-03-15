# !/usr/bin/env python
# -*- coding:utf-8 -*-
import saiorm


def test(DB, table):
    # test native function
    res = DB.select("`ABS(-2)")
    # print(res)
    print(DB.last_query)

    res = DB.select("`SUM(1+2)")
    # print(res)
    print(DB.last_query)

    # Normal usage
    res = table.select()
    print(res)
    print(DB.last_query)

    res = table.order_by("a DESC").get()
    # print(res)
    print(DB.last_query)

    res = table.limit("1,3").select("id,a")
    # print(res[0])
    print(DB.last_query)
    # raise

    res = table.where({
        "a": 1,
        "b": ("BETWEEN", "1", "2"),
        "c": ("`ABS(?)", "2"),
        "d": ("!=", 0),
        "e": ("IN", ["1","2","3"]),
        "f": "`ABS(-2)",
    }).select("e,f")
    # print(res)
    print(DB.last_query)

    res = table.where({
        "a": 1,
        "b": ("OR", "BETWEEN", "1", "2"),
        "c": ("OR", "`ABS(?)", "2"),
        "d": ("IS NOT", "NULL"),
        "e": ("OR", "NOT IN", ["1","2","3"]),
        "f": "`ABS(-2)",
    }).select("e,f")
    # print(res)
    print(DB.last_query)

    res = table.where({
        "a": ("IN", ["1", "2", "3"]),
        "b": ("`ABS(?)", "2"),
    }).update({
        "c": "`ABS(2)",
        "d": ("`ABS(?)", 3),
        "e": "2",
    })
    # print(res)
    print(DB.last_query)

    res = table.insert({
        "a": "1",
        "b": "2",
    })
    # print(res)
    print(DB.last_query)

    res = table.insert({
        "fields": ["a", "b"],
        "values": ["1", "2"],
    })
    # print(res)
    print(DB.last_query)

    # use list
    res = table.insert_many([{
        "a": "1",
        "b": "2",
    }, {
        "a": "3",
        "b": "4",
    }, {
        "a": "5",
        "b": "6",
    }])
    # print(res)
    print(DB.last_query)

    # use dict
    res = table.insert_many({
        "fields": ["a", "b"],
        "values": [
            ["1", "2"],
            ["3", "4"],
            ["5", "6"]
        ]
    })
    # print(res)
    print(DB.last_query)

    res = table.where({
        "a": "1",
        "b": "2",
        "c": ("`ABS(?)", "2"),
    }).delete()
    # print(res)
    print(DB.last_query)

    res = table.increase("a", 1)
    # print(res)
    print(DB.last_query)

    res = table.where({"c": 1}).decrease("b", 1)
    # print(res)
    print(DB.last_query)

    # res = table.get_fields_name()
    # # print(res)
    # print(DB.last_query)

    # res = table.delete()
    # # print(res)
    # print(DB.last_query)
    #
    # res = DB("xxx", strict=False).delete()
    # # print(res)
    # print(DB.last_query)


DB_type_list = []
DB_list = []
table_list = []

# MySQL
DB = saiorm.init()
DB.connect({"host": "127.0.0.1", "port": 3306, "database": "x", "user": "root", "password": "root"})
table = DB.table("xxx")
DB_type_list.append("MySQL")
DB_list.append(DB)
table_list.append(table)

# PostgreSQL
DB = saiorm.init(driver="PostgreSQL")
DB.connect({"host": "127.0.0.1", "port": "5432", "database": "x", "user": "postgres", "password": "123"})
table = DB.table("xxx")

DB_type_list.append("MySQL")
DB_list.append(DB)
table_list.append(table)

# SQLServer
DB = saiorm.init(driver="SQLServer")
DB.connect({"host": "127.0.0.1", "port": "1433", "database": "x", "user": "root", "password": "123"},
           return_query=True)
table = DB.table("xxx", primary_key="id")  # For LIMIT implement with SQL Server

DB_type_list.append("SQLServer")
DB_list.append(DB)
table_list.append(table)

# SQLite
DB = saiorm.init(driver="SQLite")
DB.connect({"host": "test.db"}, return_query=True)
# DB.execute('''CREATE TABLE xxx (id int, a int, b int, c int, d int, e int, f int)''')
# DB.execute("drop table xxx")
table = DB.table("xxx")

DB_type_list.append("SQLite")
DB_list.append(DB)
table_list.append(table)

# MongoDB
DB = saiorm.init(driver="MongoDB")
DB.connect({"host": "127.0.0.1", "port": "27017", "database": "x", "user": "", "password": ""}, return_query=True)
table = DB.table("xxx")

DB_type_list.append("MongoDB")
DB_list.append(DB)
table_list.append(table)

for index, i in enumerate(DB_type_list):
    print("-" * 30 + "TEST::" + i + "-" * 30)
    test(DB_list[index], table_list[index])
