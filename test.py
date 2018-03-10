# !/usr/bin/env python
# -*- coding:utf-8 -*-
import saiorm

DB = saiorm.CoherentDB()
DB.connect({"host": "127.0.0.1", "port": 3306, "database": "x", "user": "root", "password": "root"})

DB_table = DB.table("xxx")


def test_sql():
    res = DB_table.select()
    # print(res)
    print(DB.last_sql)

    res = DB_table.order_by("id DESC").get()
    # print(res)
    print(DB.last_sql)

    res = DB_table.where({
        "a": 1,
        "b": ("BETWEEN", "1", "2"),
        "c": ("ABS({})", "2"),
        "d": ("!=", 0),
        "e": ("IN", "1,2,3"),
        "f": "now()",
    }).select("e,f")
    # print(res)
    print(DB.last_sql)

    res = DB.select("now()")
    # print(res)
    print(DB.last_sql)

    res = DB_table.where({
        "a": 1,
        "b": 2,
        "c": ("ABS({})", "2"),
        "d": "now()",
    }).update({
        "e": "1",
        "f": "2",
    })
    # print(res)
    print(DB.last_sql)

    res = DB_table.insert({
        "a": "1",
        "b": "2",
    })
    # print(res)
    print(DB.last_sql)

    res = DB_table.insert({
        "fields": ["a", "b"],
        "values": ["1", "2"],
    })
    # print(res)
    print(DB.last_sql)

    # use list
    res = DB_table.insert_many([{
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
    res = DB_table.insert_many({
        "fields": ["a", "b"],
        "values": [
            ["1", "2"],
            ["3", "4"],
            ["5", "6"]
        ]
    })
    # print(res)
    print(DB.last_sql)

    res = DB_table.where({
        "a": "1",
        "b": "2",
        "c": ("ABS({})", "2"),
        "d": "now()",
    }).delete()
    # print(res)
    print(DB.last_sql)

    res = DB_table.increase("a", 1)
    # print(res)
    print(DB.last_sql)

    res = DB_table.decrease("a", 1)
    # print(res)
    print(DB.last_sql)

    res = DB_table.get_fields_name()
    # print(res)
    print(DB.last_sql)

    # res = DB_table.delete()
    # # print(res)
    # print(DB.last_sql)
    #
    # res = DB("xxx", strict=False).delete()
    # # print(res)
    # print(DB.last_sql)


test_sql()
