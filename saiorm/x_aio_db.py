#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
实现一个可以进行连贯操作的小 orm
参考 https://www.liaoxuefeng.com/wiki/0014316089557264a6b348958f449949df42a6d3a2e542c000/0014323389656575142d0bcfeec434e9639a80d3684a7da000

todo 参数里的 原生函数没做,insert 的部分在支持多种数据库时可能会导致复杂度太高
todo 表前缀没做

异步的支持略复杂,先不实现,只是用普通方式实现功能.

"""
import logging

db = None  # 应该是个已连接的 x_torndb.Connection,使用中覆盖


def set_db(database):
    global db
    db = database


class CoherentDB():
    """支持连贯操作"""

    def __init__(self, table_name_prefix="", debug=False, strict=True, cache_fields_name=True):
        self.table_name_prefix = table_name_prefix
        self.debug = debug
        self.strict = strict
        self._cache_fields_name = cache_fields_name # when call get_fields_name
        self._cached_fields_name = {}  # cached fields name
        self._table = ""
        self._where = ""
        self._order_by = ""
        self._group_by = ""
        self._limit = ""
        self._inner_join = ""
        self._left_join = ""
        self._right_join = ""
        self._on = ""
        self.last_sql = ""  # latest executed sql

    def _reset(self):
        """reset param when call again"""
        self._table = ""
        self._where = ""
        self._order_by = ""
        self._group_by = ""
        self._limit = ""
        self._inner_join = ""
        self._left_join = ""
        self._right_join = ""
        self._on = ""
        self.last_sql = ""  # latest executed sql

    def table(self, table_name=""):
        """
        If table_name is empty,use DB().select("now()") will run SELECT now()
        """
        self._reset()  # reset param
        # check table name prefix
        if self.table_name_prefix and not table_name.startswith(self.table_name_prefix):
            table_name += self.table_name_prefix

        self._table = table_name
        return self

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

    def __gen_condition(self):
        """generate query condition"""
        res = ""
        if self._where:
            where = self._where
            if isinstance(self._where, dict):
                where = ""
                for k in self._where.keys():
                    v = self._where[k]
                    if isinstance(v, tuple) or isinstance(v, list):
                        v = v[0].format(*v[1:])
                    s = " {}={} AND".format(k, str(v))
                    where += s
                if where:
                    where = where[:-3]  # trim the last  AND character
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

    def __gen_kv_str(self, dict_data):
        """
        generate str ike filed_name = %s and values,use for select and update
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

    def select(self, fields="*"):
        """
        fields is fields or native sql function,
        ,use DB().select("now()") will run SELECT now()
        """
        if self._table and fields:
            sql = "SELECT {} FROM {} {};".format(fields, self._table, self.__gen_condition())
        else:
            sql = "SELECT {};".format(fields)  # 用于直接执行 mysql 函数

        res = db.query_with_detail(sql)
        self.last_sql = res["sql"]
        return res["data"]

    def get(self, fields="*"):
        """will replace self._limit to 1"""
        self._limit = "1"
        return self.select(fields)

    def update(self, dict_data=None):
        if not dict_data:
            return

        fields, values = self.__gen_kv_str(dict_data)
        sql = "UPDATE {} SET {} {};".format(self._table, fields, self.__gen_condition())
        values = tuple(values)
        res = db.execute_with_detail(sql, *values)
        self.last_sql = res["sql"]
        return res

    def insert(self, dict_data=None):
        """
        insert one line,support rwo kinds data::

        1. dict with key fields and values,the values of keys are list or tuple
        respectively all field name and value

        2. dict, field name and value

        """
        if not dict_data:
            return

        # todo 原生函数的未处理

        keys = dict_data.keys()
        if "fields" in keys and "values" in keys:  # 字段名和值分开传
            fields = ",".join(dict_data["fields"])
            values = [v for v in dict_data["values"]]
        else:  # 普通字典
            fields = ",".join(keys)
            values = dict_data.values()

        values_sign = ",".join(["%s" for i in values])
        if fields:
            sql = "INSERT INTO {} ({}) VALUES ({});".format(self._table, fields, values_sign,
                                                            self.__gen_condition())
        else:
            sql = "INSERT INTO {} VALUES ({});".format(self._table, values_sign, self.__gen_condition())
        values = tuple(values)
        res = db.execute_with_detail(sql, *values)
        self.last_sql = res["sql"]
        return res

    def insert_many(self, dict_data=None):
        """
        insert one line,,support rwo kinds data,such as insert,
        but the values should be wraped with list or tuple

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
            values = [tuple(i.values()) for i in dict_data]  # 字典的 values 先转换
            values_sign = ",".join(["%s" for f in keys])
        elif isinstance(dict_data, dict):  # 字段名和值分开传
            keys = dict_data.get("fields")
            if keys:  # 允许不指定字段
                fields = ",".join(dict_data["fields"])
            values = list([v for v in dict_data["values"]])  # 字典的 values 先转换
            values_sign = ",".join(["%s" for f in keys])
        else:
            logging.error("Param should be list or tuple or dict")
            return

        if fields:
            sql = "INSERT INTO {} ({}) VALUES ({});".format(self._table, fields, values_sign)
        else:
            sql = "INSERT INTO {}  VALUES ({});".format(self._table, values_sign)

        res = db.executemany_with_detail(sql, values)
        self.last_sql = res["sql"]
        return res

    def delete(self):
        if self.strict and not self._where:  # 没有 where 条件禁止执行
            logging.warning("with no where condition,can not delete")
            return

        sql = "DELETE FROM {} {};".format(self._table, self.__gen_condition())

        res = db.execute_with_detail(sql)
        self.last_sql = res["sql"]
        return res

    def increase(self, field, step=1):
        """number field Increase """
        sql = "UPDATE {} SET {}={}+{}".format(self._table, field, field, str(step))
        # print(sql)
        # raise
        res = db.execute_with_detail(sql)
        self.last_sql = res["sql"]
        return res

    def decrease(self, field, step=1):
        """number field decrease """
        sql = "UPDATE {} SET {}={}-{}".format(self._table, field, field, str(step))
        res = db.execute_with_detail(sql)
        self.last_sql = res["sql"]
        return res

    def get_fields_name(self):
        """return all fields of table"""
        if not self._table:
            return []

        if self._cache_fields_name and self._cached_fields_name.get(self._table):
            return self._cached_fields_name.get(self._table)
        else:
            res = db.query_with_detail("SELECT * FROM xxx LIMIT 1")
            fields_name = res["column_names"]
            self._cached_fields_name[self._table] = fields_name

            return fields_name


