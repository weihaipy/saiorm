#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Support SQLServer

bases on torndb
"""
import logging
import time

import pymssql

try:
    from . import utility
except ImportError:
    import utility

try:
    from . import base
except ImportError:
    import base

Row = utility.Row
GraceDict = utility.GraceDict
is_array = utility.is_array
to_unicode = utility.to_unicode


class ConnectionSQLServer(object):
    def __init__(self, host, port, database, user=None, password=None,
                 max_idle_time=7 * 3600):
        self.host = host
        self.database = database
        self.max_idle_time = float(max_idle_time)

        args = dict(
            host=host,
            port=str(port),
            user=user,
            password=password,
            database=database,
            # charset=charset,
            # use_unicode=True,
            # init_command=('SET time_zone = "%s"' % time_zone),
            # connect_timeout=connect_timeout,
            # **kwargs
        )

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except Exception:
            logging.error("Cannot connect to SQLServer on {}:{}".format(self.host, port),
                          exc_info=True)

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def reconnect(self):
        """Closes the existing database connection and re-opens it.
        改用 pymssql 实现"""
        self.close()

        self._db = pymssql.connect(**self._db_args)
        self._db.autocommit(True)

    def iter(self, query, *parameters, **kwparameters):
        """Returns an iterator for the given query and parameters."""
        self._ensure_connected()
        # cursor = cursors.SSCursor(self._db) # psycopg2 没有 cursors
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    def _ensure_connected(self):
        # Mysql by default closes client connections that are idle for
        # 8 hours, but the client library does not report this fact until
        # you try to perform a query and it fails.  Protect against this
        # case by preemptively closing and reopening the connection
        # if it has been idle for too long (7 hours by default).
        if (self._db is None or
                (time.time() - self._last_use_time > self.max_idle_time)):
            self.reconnect()
        self._last_use_time = time.time()

    def _cursor(self):
        self._ensure_connected()
        return self._db.cursor()

    def _execute(self, cursor, query, parameters, kwparameters):
        try:
            return cursor.execute(query, kwparameters or parameters)
        except Exception as e:
            logging.error("Error connecting to SQLServer on %s", self.host)
            logging.error("Error query: %s", query)
            logging.error("Error parameters: %s", parameters)
            logging.error("Error kwparameters: %s", kwparameters)
            self.close()
            print("SQLServer Error Info:", e)
            raise

    def query_return_detail(self, query, *parameters, **kwparameters):
        """return_detail"""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]

            return {
                "data": [Row(zip(column_names, row)) for row in cursor.fetchall()],
                "column_names": column_names,
                "sql": ""  # 执行的语句
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
                "sql": ""  # 执行的语句
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
                "sql": ""  # 执行的语句
            }
        finally:
            cursor.close()


class ChainDB(base.ChainDB):
    def connect(self, config_dict=None):
        self.db = ConnectionSQLServer(**config_dict)

    def select(self, fields="*"):
        """
        fields is fields or native sql function,
        ,use DB().select("=now()") will run SELECT now()
        """
        condition_values = []
        pre_sql = ""
        pre_where = ""

        if fields.startswith("`"):  # native function
            sql = self.gen_select_without_fields(fields[1:])
        else:
            # implement LIMIT here
            if self._limit:
                _limit = str(self._limit)

                if "," not in _limit:
                    pre_sql = "SELECT TOP {} {} FROM {} ".format(_limit, fields, self._table)
                else:
                    m, n = _limit.split(",")
                    if self._where:
                        param = {
                            "m": m,
                            "fields": fields,
                            "table": self._table,
                            "pk": self._primary_key
                        }
                        pre_where = "WHERE {pk} NOT IN (SELECT TOP {m}-1 {pk} FROM {table})".format(param)
                        self._where = None  # clean self._where
                    else:
                        param = {
                            "m": m,
                            "n": n,
                            "fields": fields,
                            "table": self._table,
                            "pk": self._primary_key
                        }
                        pre_sql = "SELECT TOP ({n}-{m}+1) {fields} FROM {table} " \
                                  "WHERE {pk} NOT IN (SELECT TOP {m}-1 {pk} FROM {table})".format(**param)
                self._limit = None  # clean self._limit
            else:
                pre_sql = "SELECT {} FROM {} ".format(fields, self._table)

            condition_sql, condition_values = self.parse_condition()

            if pre_where:
                if condition_sql.startswith("WHERE"):
                    condition_sql = pre_where + " AND " + condition_sql[len("WHERE"):]
                else:
                    condition_sql = pre_where + " AND " + condition_sql

            sql = pre_sql + condition_sql

        res = self.query(sql, *condition_values)
        self.last_sql = res["sql"]
        if self.grace_result:
            res["data"] = [GraceDict(i) for i in res["data"]]

        return res["data"]

    def gen_get_fields_name(self):
        """get one line from table"""
        return "SELECT TOP 1 * FROM {};".format(self._table)
