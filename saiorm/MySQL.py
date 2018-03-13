#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Support MySQL
"""
import ast

try:
    from . import utility
except ImportError:
    import utility

try:
    from . import base
except ImportError:
    import base

try:
    import x_torndb
except ImportError:
    from . import x_torndb

torndb = x_torndb
Row = torndb.Row
GraceDict = utility.GraceDict
is_array = utility.is_array
to_unicode = utility.to_unicode


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
                "sql": to_unicode(cursor._executed)  # 执行的语句
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
                "sql": to_unicode(cursor._executed)  # 执行的语句
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
                "sql": to_unicode(cursor._executed)  # 执行的语句
            }
        finally:
            cursor.close()


class ChainDB(base.ChainDB):
    def connect(self, config_dict=None):
        self.db = ConnectionMySQL(**config_dict)
