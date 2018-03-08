# !/usr/bin/env python
# -*- coding:utf-8 -*-
import saiorm

saiorm.set_db(saiorm.Connection(host="127.0.0.1", port=3306, database="x", user="root", password="root"))
DB = saiorm.CoherentDB


def test_sql():
    res = DB("xxx").select()
    print(res)

    res = DB("xxx").order_by("id DESC").get()
    print(res)

    res = DB("xxx").where({
        "a": 1,
        "b": 2,
        "c": ("ABS({})", "2"),
        "d": "now()",
    }).select("e,f")
    print(res)

    res = DB().select("now()")
    print(res)

    res = DB("xxx").where({
        "a": 1,
        "b": 2,
        "c": ("ABS({})", "2"),
        "d": "now()",
    }).update({
        "e": "1",
        "f": "2",
    })
    print(res)

    res = DB("xxx").insert({
        "a": "1",
        "b": "2",
    })
    print(res)

    res = DB("xxx").insert({
        "fields": ["a", "b"],
        "values": ["1", "2"],
    })
    print(res)

    # use list
    res = DB("xxx").insert_many([{
        "a": "1",
        "b": "2",
    }, {
        "a": "3",
        "b": "4",
    }])
    print(res)

    # use dict
    res = DB("xxx").insert_many({
        "fields": ["a", "b"],
        "values": [
            ["1", "2"],
            ["3", "4"],
            ["5", "6"]
        ]
    })
    print(res)

    res = DB("xxx").where({
        "a": 1,
        "b": 2,
        "c": ("ABS({})", "2"),
        "d": "now()",
    }).delete()
    print(res)

    # res = DB("xxx").delete()
    # print(res)
    #
    # res = DB("xxx", strict=False).delete()
    # print(res)


test_sql()
