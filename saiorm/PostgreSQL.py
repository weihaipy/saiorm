#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Support PostgreSQL

bases on torndb
"""
import logging
import time

import psycopg2

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


class Connection(base.BaseConnection):
    def __init__(self, host, port, database, user=None, password=None,
                 max_idle_time=7 * 3600):
        self.host = host
        self.database = database
        self.max_idle_time = float(max_idle_time)

        args = dict(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database,
        )

        self.db = None
        self.db_args = args
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except Exception:
            logging.error(f"Cannot connect to PostgreSQL on {self.host}:{port}",
                          exc_info=True)

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.close()

        self.db = psycopg2.connect(**self.db_args)
        self.db.set_session(autocommit=True)  # psycopg2 的设置方法不一样

    def iter(self, query, *parameters, **kwparameters):
        """Returns an iterator for the given query and parameters."""
        self._ensure_connected()
        # cursor = cursors.SSCursor(self.db) # psycopg2 没有 cursors
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    def _log_exception(self, exception, query, parameters):
        """log exception when execute SQL"""
        pass

    def query_return_detail(self, query, *parameters, **kwparameters):
        """return_detail"""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]

            return {
                "data": [Row(zip(column_names, row)) for row in cursor],
                "column_names": column_names,
                "query": to_unicode(cursor.query)  # query executed
            }
        finally:
            cursor.close()

    def execute_return_detail(self, query, *parameters, **kwparameters):
        """return_detail"""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return {
                "lastrowid": cursor.lastrowid,  # the primary key id affected
                "rowcount": cursor.rowcount,  # number of rows affected
                "rownumber": cursor.rownumber,  # line number
                "query": to_unicode(cursor.query)  # query executed
            }
        finally:
            cursor.close()

    def executemany_return_detail(self, query, parameters):
        """return_detail"""
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return {
                "lastrowid": cursor.lastrowid,  # the primary key id affected
                "rowcount": cursor.rowcount,  # number of rows affected
                "rownumber": cursor.rownumber,  # line number
                "query": to_unicode(cursor.query)  # query executed
            }
        except Exception as e:
            self._log_exception(e, query, parameters)
            self.close()
            raise
        finally:
            cursor.close()


class ChainDB(base.ChainDB):
    field_name_quote = '"'

    def connect(self, config_dict=None):
        self.connection = Connection(**config_dict)

    def parse_limit(self, sql):
        """parse limit condition"""

        if self._limit == 0:
            return sql

        # PostgreSQL LIMIT is different from MySQL
        if self._limit:
            if isinstance(self._limit, str) and "," in self._limit:
                m, n = self._limit.replace(" ", "").split(",")
                sql += f" LIMIT {n} OFFSET {m}"
            elif self._offset:
                sql += f" LIMIT {self._limit} OFFSET {self._offset}"
            else:
                sql += f" LIMIT {self._limit} "

        return sql
