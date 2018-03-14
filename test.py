# !/usr/bin/env python
# -*- coding:utf-8 -*-
import saiorm

# MySQL
DB = saiorm.init()
DB.connect({"host": "127.0.0.1", "port": 3306, "database": "x", "user": "root", "password": "root"})
table = DB.table("xxx")

# PostgreSQL
# DB = saiorm.init(driver="PostgreSQL")
# DB.connect({"host": "127.0.0.1", "port": "5432", "database": "x", "user": "postgres", "password": "123"})
# table = DB.table("xxx")

# SQLServer
# DB = saiorm.init(driver="SQLServer")
# DB.connect({"host": "127.0.0.1", "port": "1433", "database": "x", "user": "root", "password": "123"},
#            return_query=True)
# table = DB.table("xxx", primary_key="id")  # For LIMIT implement with SQL Server


# MongoDB
DB = saiorm.init(driver="MongoDB")
DB.connect({"host": "127.0.0.1", "port": "27017", "database": "x", "user": "postgres", "password": "123"})
table = DB.table("xxx")


# test native function
res = DB.select("`ABS(-2)")
# print(res)
print(DB.last_query)

res = DB.select("`SUM(1+2)")
# print(res)
print(DB.last_query)

# Normal usage
res = table.select()
# print(res)
print(DB.last_query)

res = table.order_by("a DESC").get()
# print(res)
print(DB.last_query)

res = table.limit("1,3").select("id,a")
# print(res)
print(DB.last_query)

res = table.where({
    "a": 1,
    "b": ("BETWEEN", "1", "2"),
    "c": ("`ABS(?)", "2"),
    "d": ("!=", 0),
    "e": ("IN", "1,2,3"),
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

# todo mmsql de insert 不能都转为元组,可能导致别的问题
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

res = table.decrease("a", 1)
# print(res)
print(DB.last_query)

res = table.get_fields_name()
# print(res)
print(DB.last_query)

# res = table.delete()
# # print(res)
# print(DB.last_query)
#
# res = DB("xxx", strict=False).delete()
# # print(res)
# print(DB.last_query)
