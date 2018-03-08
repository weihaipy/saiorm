#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
实现一个可以进行连贯操作的小 orm
参考 https://www.liaoxuefeng.com/wiki/0014316089557264a6b348958f449949df42a6d3a2e542c000/0014323389656575142d0bcfeec434e9639a80d3684a7da000

todo 参数里的 原生函数没做,insert 的部分在支持多种数据库时可能会导致复杂度太高
todo 表前缀没做

异步的支持略复杂,先不实现,只是用普通方式实现功能.

"""

db = None  # 应该是个已连接的 x_torndb.Connection,使用中覆盖


def set_db(database):
    global db
    db = database


class CoherentDB():
    """支持连贯操作"""

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

    def select(self, fields="*"):
        if self.table_name:
            sql = "SELECT {} FROM {} {};".format(fields, self.table_name, self.__gen_condition())
        else:
            sql = "SELECT {};".format(fields)  # 用于直接执行 mysql 函数

        return db.query(sql)

    def get(self, fields="*"):
        self._limit = "1"
        return self.select(fields)

    def update(self, dict_data=None):
        if not dict_data:
            return

        fields, values = self.__gen_kv_with_native(dict_data)
        sql = "UPDATE {} SET {} {};".format(self.table_name, fields, self.__gen_condition())
        values = tuple(values)
        return db.update(sql, *values)

    def insert(self, dict_data=None):
        # 插入单行
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
        values = tuple(values)
        return db.execute_with_detail(sql, *values)

    def insert_many(self, dict_data=None):
        # 插入多行
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
            values_sign = ",".join(["%s" for i in keys])
        elif isinstance(dict_data, dict):  # 字段名和值分开传
            if dict_data.get("fields"):  # 允许不指定字段
                fields = ",".join(dict_data["fields"])
            values = list([v for v in dict_data["values"]])  # 字典的 values 先转换
            values_sign = ",".join(["%s" for v in values])
        else:
            raise ValueError("Param Error")

        if fields:
            sql = "INSERT INTO {} ({}) VALUES ({});".format(self.table_name, fields, values_sign)
        else:
            sql = "INSERT INTO {}  VALUES ({});".format(self.table_name, values_sign)

        # todo TypeError: not enough arguments for format string 但是查看了一下,没问题

        # values = [tuple(i) for i in values]/
        print("------------insert  many------")
        print("sql:", sql)
        print("values::", values)
        return db.executemany_with_detail(sql, values)

    def delete(self):
        if self.strict and not self._where:  # 没有 where 条件禁止执行
            if self.debug:
                print("没有 where 条件,不可以 delete ")
            return

        sql = "DELETE FROM {} {};".format(self.table_name, self.__gen_condition())
        return db.execute_with_detail(sql)

    def increase(self, field, step):
        # 数字字段增加
        # todo 执行  update 执行 update BBD set cs=cs+1
        pass

    def decrease(self, field, step):
        # 数字字段减少
        pass
