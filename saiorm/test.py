#!/usr/bin/env python
# -*- coding:utf-8 -*-
#
import x_aio_db

DB = x_aio_db.DB
#
#
# def test_sql():
#     DB("table").select()
#
#     DB("table").order_by("id DESC").get()
#
#     DB("table").where({
#         "a": 1,
#         "b": 2,
#         "c": ("ABS({})", "2"),
#         "d": "now()",
#     }).select("zz,xx")
#
#     DB().select("now()")
#
#     DB("table").where({
#         "a": 1,
#         "b": 2,
#         "c": ("ABS({})", "2"),
#         "d": "now()",
#     }).update({
#         "x": "1",
#         "y": "2",
#     })
#
#     DB("table").insert({
#         "a": "1",
#         "b": "2",
#     })
#
#     DB("table").insert({
#         "fields": ["a", "b"],
#         "values": ["1", "2"],
#
#     })
#
#     # use list
#     DB("table").insert_many([{
#         "a": "1",
#         "b": "2",
#     }, {
#         "a": "3",
#         "b": "4",
#     }])
#
#     DB("table").insert_many([{
#         "a": "1",
#         "b": "2",
#     }, {
#         "a": "3",
#         "b": "4",
#     }], one_line=False)
#
#     # use dict
#     DB("table").insert_many({
#         "fields": ["a", "b"],
#         "values": [
#             ["1", "2"],
#             ["3", "4"],
#             ["5", "6"]
#         ]
#     })
#
#     DB("table").insert_many({
#         "fields": ["a", "b"],
#         "values": [
#             ["1", "2"],
#             ["3", "4"],
#             ["5", "6"]
#         ]
#     }, one_line=False)
#
#     DB("table").where({
#         "a": 1,
#         "b": 2,
#         "c": ("ABS({})", "2"),
#         "d": "now()",
#     }).delete()
#
#     DB("table").delete()
#     DB("table", strict=False).delete()
#
#
x_aio_db.create_pool_mysql({
    "host": "127.0.0.1",
    "user": "root",
    "password": "root",
    "db": "xxxx"

})
r = DB().select()

print(r)

r = DB("xxx").insert({
    "qq": "1234",
    "ww": "7689"
})

print(r)

#
# import asyncio
# import aiomysql
#
# loop = asyncio.get_event_loop()
#
#
# @asyncio.coroutine
# def test_example():
#     conn = yield from aiomysql.connect(host='127.0.0.1', port=3306,
#                                        user='root', password='root', db='x',
#                                        loop=loop)
#
#     cur = yield from conn.cursor()
#     yield from cur.execute("SELECT *  FROM xxx")
#     print(cur.description)
#     # column_names = [d[0] for d in cur.description] # 列明
#     r = yield from cur.fetchall()
#     print(r)
#     yield from cur.close()
#     conn.close()
#
#
# loop.run_until_complete(test_example())
