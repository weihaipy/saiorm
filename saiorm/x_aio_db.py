#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
实现一个可以进行连贯操作的小 orm
参考 https://www.liaoxuefeng.com/wiki/0014316089557264a6b348958f449949df42a6d3a2e542c000/0014323389656575142d0bcfeec434e9639a80d3684a7da000

todo 参数里的 原生函数没做,insert 的部分在支持多种数据库时可能会导致复杂度太高

使用 aioodbc 可以支持更多的数据库,先使用 aiomysql 实现 mysql 的功能,再考虑 aiopg 还是 aioodbc 支持其他类型的数据库

"""
import logging
import asyncio

# import aioodbc
import aiomysql

# 连接池
__pool = None

loop = asyncio.get_event_loop()


def log(sql, args=()):
    logging.info('SQL: %s' % sql)


@asyncio.coroutine
def create_pool_mysql(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = yield from aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )


class DB():
    def __init__(self, table_name="", debug=False, strict=True):
        self.debug = debug
        self.strict = strict
        self.table_name = table_name  # 为空可以直接使用 mysql 函数,如 now()
        self._where = ""
        self._order_by = ""
        self._group_by = ""
        self._limit = ""
        self._inner_join = ""
        self._left_join = ""
        self._right_join = ""
        self._on = ""
        # self.last_sql = "" # 最后执行的 SQL 语句

    def where(self, condition):
        self._where = condition
        return self

    def order_by(self, condition):
        self._order_by = condition
        return self

    def limit(self, condition):
        self._limit = condition
        return self

    def group_by(self, condition):
        self._group_by = condition
        return self

    def join(self, condition):
        self._inner_join = condition
        return self

    def inner_join(self, condition):
        self._inner_join = condition
        return self

    def left_join(self, condition):
        self._left_join = condition
        return self

    def right_join(self, condition):
        self._right_join = condition
        return self

    def on(self, condition):
        self._on = condition
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global loop
        loop.stop()

    def __gen_condition(self):
        """
        生成条件
        :return: str
        """
        res = ""
        if self._where:
            where = self._where
            # 字典参数的整理一下
            if isinstance(self._where, dict):
                where = ""
                for k in self._where.keys():
                    v = self._where[k]
                    if isinstance(v, tuple) or isinstance(v, list):
                        v = v[0].format(*v[1:])
                    s = " {}={} AND".format(k, str(v))
                    where += s
                if where:
                    where = where[:-3]
            res += "WHERE" + where

        if self._on:
            if self._inner_join:
                res += " INNER JOIN {} ON {}".format(self._inner_join, self._on)
            elif self._left_join:
                res += " LEFT JOIN {} ON {}".format(self._left_join, self._on)
            elif self._right_join:
                res += " RIGHT JOIN {} ON {}".format(self._right_join, self._on)

        if self._order_by:
            res += " ORDER BY " + self._order_by
        if self._limit:
            res += " LIMIT " + str(self._limit)
        if self._group_by:
            res += " GROUP BY " + self._group_by
        return res

    def __gen_kv_with_native(self, dict_data):
        """
        生成字段键值对的形式,用于 select
        :return: tuple
        """
        fields = ""
        values = []
        for k in dict_data.keys():
            v = dict_data[k]
            # 改用 where 的方法
            # if v.startswith("="):  # 使用 mysql 原生函数的
            #     v = v[1:]
            #     fields += k + "=" + v + ","
            # else:
            #     fields += k + ","
            fields += k + "=%s,"
            values.append(v)
        if fields:
            fields = fields[:-1]

        return fields, values

    @asyncio.coroutine
    def exec(self, sql, args):
        global __pool
        with (yield from __pool) as conn:
            try:
                cur = yield from conn.cursor()
                yield from cur.execute(sql.replace('?', '%s'), args)
                rowcount = cur.rowcount  # 影响行数
                lastrowid = cur.lastrowid  # 最后的主键
                rownumber = cur.rownumber  # 行号
                if self.debug:
                    print(cur._executed)
                yield from cur.close()
            except BaseException as e:
                raise
            return {
                "rowcount": rowcount,
                "lastrowid": lastrowid,
                "rownumber": rownumber,
            }

    @asyncio.coroutine
    def __query(self, sql):
        log(sql)
        global __pool
        with (yield from __pool) as conn:
            try:
                print(sql)
                cur = yield from conn.cursor()
                yield from cur.execute(sql)
                if self.debug:
                    print(cur._executed)
                # print(cur.description)

                print(cur.description)
                res = yield from cur.fetchall()
                print(res)
                yield from cur.close()
                conn.close()
            except BaseException as e:
                raise

            print("res:", res)
            return res

    # @asyncio.coroutine
    # def select(self, fields="*"):
    #     if self.table_name:
    #         sql = "SELECT {} FROM {} {};".format(fields, self.table_name, self.__gen_condition())
    #     else:
    #         sql = "SELECT {};".format(fields)  # 用于直接执行 mysql 函数
    #     return self.__query(sql)

    async def select(self, fields="*", args=[], size=None):

        if self.table_name:
            sql = "SELECT {} FROM {} {};".format(fields, self.table_name, self.__gen_condition())
        else:
            sql = "SELECT {};".format(fields)  # 用于直接执行 mysql 函数

        log(sql, args)
        global __pool
        async with __pool.get() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args or ())
                if size:
                    rs = await cur.fetchmany(size)
                else:
                    rs = await cur.fetchall()
            logging.info('rows returned: %s' % len(rs))
            return rs

    @asyncio.coroutine
    def get(self, fields="*"):
        # todo 考虑使用 fetchone 执行
        self._limit = "1"
        return self.select(fields)

    @asyncio.coroutine
    def update(self, dict_data=None):
        if not dict_data:
            return

        fields, values = self.__gen_kv_with_native(dict_data)
        sql = "UPDATE {} SET {} {};".format(self.table_name, fields, self.__gen_condition())
        self.exec(sql, values)

    @asyncio.coroutine
    def insert(self, dict_data=None):
        """
        插入单行
        :param dict_data:
        """
        if not dict_data:
            return

        # todo 原生函数的未处理

        # 区分传递的字典结构
        keys = dict_data.keys()
        if "fields" in keys and "values" in keys:  # 字段名和值分开传
            fields = ",".join(dict_data["fields"])
            values = [v for v in dict_data["values"]]
        else:  # 普通字典
            fields = ",".join(keys)
            values = dict_data.values()

        values_sign = ",".join(["%s" for i in values])
        if fields:
            sql = "INSERT INTO {} ({}) VALUES ({});".format(self.table_name, fields, values_sign,
                                                            self.__gen_condition())
        else:
            sql = "INSERT INTO {} VALUES ({});".format(self.table_name, values_sign, self.__gen_condition())
        self.exec(sql, values)

    @asyncio.coroutine
    def insert_many(self, dict_data=None, one_line=True):
        """

        :param dict_data:
        :param one_line: bool,是否使用一行语句,不建议传入超长数据
        :return:

        todo  考虑使用 executemany 执行
        """
        if not dict_data:
            return

        fields = ""  # 所有的字段
        values = []  # 所有的参数
        values_sign = ""  # 参数的占位符

        # 列表或元祖的结构必须一样
        if isinstance(dict_data, list) or isinstance(dict_data, tuple):
            dict_data_item_1 = dict_data[0]  # 应该是字典
            keys = dict_data_item_1.keys()
            fields = ",".join(keys)
            values = [i.values for i in dict_data]
            values_sign = ",".join(["%s" for i in keys])
        elif isinstance(dict_data, dict):  # 字段名和值分开传
            if dict_data.get("fields"):  # 允许不指定字段
                fields = ",".join(dict_data["fields"])
            values = [v for v in dict_data["values"]]
            values_sign = ",".join(["%s" for v in values])
        else:
            return

        if one_line:
            if fields:
                sql = "INSERT INTO {} ({}) VALUES ({});".format(self.table_name, fields, values_sign)
            else:
                sql = "INSERT INTO {}  VALUES ({});".format(self.table_name, values_sign)
            self.exec(sql, values)
        else:
            if fields:
                for v in values:
                    sql = "INSERT INTO {} ({}) VALUES ({});".format(self.table_name, fields, values_sign)

                    self.exec(sql, v)
            else:

                for v in values:
                    sql = "INSERT INTO {} VALUE ({});".format(self.table_name, values_sign)

                    self.exec(sql, v)

    @asyncio.coroutine
    def delete(self):
        if self.strict and not self._where:  # 没有 where 条件禁止执行
            if self.debug:
                print("没有 where 条件,不可以 delete ")
            return

        sql = "DELETE FROM {} {};".format(self.table_name, self.__gen_condition())
        self.exec(sql, ())

    @asyncio.coroutine
    def increase(self, field, step):
        # 数字字段增加
        # todo 执行  update 执行
        pass

    @asyncio.coroutine
    def decrease(self, field, step):
        # 数字字段减少
        pass
