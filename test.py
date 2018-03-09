# !/usr/bin/env python
# -*- coding:utf-8 -*-
import saiorm

saiorm.set_db(saiorm.Connection(host="127.0.0.1", port=3306, database="x", user="root", password="root"))
DB = saiorm.CoherentDB()


def test_sql():
    res = DB.table("xxx").select()
    print(res)

    res = DB.table("xxx").order_by("id DESC").get()
    print(res)

    res = DB.table("xxx").where({
        "a": 1,
        "b": 2,
        "c": ("ABS({})", "2"),
        "d": "now()",
    }).select("e,f")
    print(res)

    res = DB.select("now()")
    print(res)

    res = DB.table("xxx").where({
        "a": 1,
        "b": 2,
        "c": ("ABS({})", "2"),
        "d": "now()",
    }).update({
        "e": "1",
        "f": "2",
    })
    print(res)

    res = DB.table("xxx").insert({
        "a": "1",
        "b": "2",
    })
    print(res)

    res = DB.table("xxx").insert({
        "fields": ["a", "b"],
        "values": ["1", "2"],
    })
    print(res)

    # use list
    res = DB.table("xxx").insert_many([{
        "a": "1",
        "b": "2",
    }, {
        "a": "3",
        "b": "4",
    }, {
        "a": "5",
        "b": "6",
    }])
    print(res)

    # use dict
    res = DB.table("xxx").insert_many({
        "fields": ["a", "b"],
        "values": [
            ["1", "2"],
            ["3", "4"],
            ["5", "6"]
        ]
    })
    print(res)

    res = DB.table("xxx").where({
        "a": 1,
        "b": 2,
        "c": ("ABS({})", "2"),
        "d": "now()",
    }).delete()
    print(res)

    res = DB.table("xxx").increase("a", 1)
    print(res)

    res = DB.table("xxx").get_fields_name()
    print(res)

    res = DB.table("xxx").get_fields_name()
    print(res)

    # res = DB.table("xxx").delete()
    # print(res)
    #
    # res = DB("xxx", strict=False).delete()
    # print(res)


test_sql()
