#!/usr/bin/env python
# -*- coding:utf-8 -*-

try:

    import x_torndb
except ImportError:
    from . import x_torndb

torndb = x_torndb
Row = torndb.Row


class ConnectionMySQL(torndb.Connection):
    def query_return_detail(self, query, *parameters, **kwparameters):
        """return_detail"""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            return {
                "data": [Row(zip(column_names, row)) for row in cursor],
                "column_names": column_names,
                "sql": cursor._executed  # 执行的语句
            }
        finally:
            cursor.close()

    def execute_return_detail(self, query, *parameters, **kwparameters):
        """return_detail"""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return {
                "lastrowid": cursor.lastrowid,  # 影响的主键id
                "rowcount": cursor.rowcount,  # 影响的行数
                "rownumber": cursor.rownumber,  # 行号
                "sql": cursor._executed  # 执行的语句
            }
        finally:
            cursor.close()

    def executemany_return_detail(self, query, parameters):
        """return_detail"""
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return {
                "lastrowid": cursor.lastrowid,  # 影响的主键id
                "rowcount": cursor.rowcount,  # 影响的行数
                "rownumber": cursor.rownumber,  # 行号
                "sql": cursor._executed  # 执行的语句
            }
        finally:
            cursor.close()


class GraceDict(dict):
    """
    Return empty string instead of throw KeyError when key is not exist.

    Friendly for web development.
    """

    def __missing__(self, name):
        # 用于 d[key]形式,没有键的情况
        return ""

    def __getitem__(self, name):
        # 用于 d[key]形式,键值为None的情况
        v = super(GraceDict, self).__getitem__(name)
        if v is not None:
            return v
        else:
            return ""

    def get(self, key, default=""):
        # 用于 d.get(key) 的形式
        if key in self:
            r = "" if self[key] is None else self[key]
            return r
        elif default:
            return default
        else:
            return ""


def is_array(obj):
    return isinstance(obj, tuple) or isinstance(obj, list)
