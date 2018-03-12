# !/usr/bin/env python
# -*- coding:utf-8 -*-
import saiorm

DB = saiorm.init()
DB.connect({"host": "127.0.0.1", "port": 3306, "database": "x", "user": "root", "password": "root"})

# test mysql function
res = DB.select("`NOW()")
# print(res)
print(DB.last_sql)

res = DB.select("`SUM(1+2)")
# print(res)
print(DB.last_sql)

# Normal usage
table = DB.table("xxx")

res = table.select()
# print(res)
print(DB.last_sql)

res = table.order_by("id DESC").get()
# print(res)
print(DB.last_sql)

res = table.where({
    "a": 1,
    "b": ("BETWEEN", "1", "2"),
    "c": ("`ABS(?)", "2"),
    "d": ("!=", 0),
    "e": ("IN", "1,2,3"),
    "f": "`NOW()",
}).select("e,f")
# print(res)
print(DB.last_sql)

res = table.where({
    "a": ("IN", ["1", "2", "3"]),
    "b": ("`ABS(?)", "2"),
}).update({
    "c": "`ABS(2)",
    "d": ("`ABS(?)", 3),
    "e": "2",
})
# print(res)
print(DB.last_sql)

res = table.insert({
    "a": "1",
    "b": "2",
})
# print(res)
print(DB.last_sql)

res = table.insert({
    "fields": ["a", "b"],
    "values": ["1", "2"],
})
# print(res)
print(DB.last_sql)

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
print(DB.last_sql)

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
print(DB.last_sql)

res = table.where({
    "a": "1",
    "b": "2",
    "c": ("`ABS(?)", "2"),
}).delete()
# print(res)
print(DB.last_sql)

res = table.increase("a", 1)
# print(res)
print(DB.last_sql)

res = table.decrease("a", 1)
# print(res)
print(DB.last_sql)

res = table.get_fields_name()
# print(res)
print(DB.last_sql)

# res = table.delete()
# # print(res)
# print(DB.last_sql)
#
# res = DB("xxx", strict=False).delete()
# # print(res)
# print(DB.last_sql)
