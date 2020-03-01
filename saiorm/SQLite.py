#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Support SQLite

bases on torndb
"""
import logging
import time

import sqlite3

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
    def __init__(self, host, return_query=False):
        self.host = host
        self._return_query = return_query

        self.db = None
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except Exception:
            logging.error(f"Cannot connect to SQLite on {self.host}",
                          exc_info=True)

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.close()
        self.db = sqlite3.connect(self.host)

    def iter(self, query, *parameters, **kwparameters):
        """Returns an iterator for the given query and parameters."""
        # self._ensure_connected()
        # cursor = cursors.SSCursor(self.db) # psycopg2 没有 cursors
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    def _ensure_connected(self):
        pass

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
                "data": [Row(zip(column_names, row)) for row in cursor.fetchall()],
                "column_names": column_names,
                "query": query.replace("?", "{}").format(*parameters) if self._return_query else ""  # query executed
            }
        finally:
            # cursor.close()
            pass

    def execute_return_detail(self, query, *parameters, **kwparameters):
        """return_detail"""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return {
                "lastrowid": cursor.lastrowid,  # the primary key id affected
                "rowcount": cursor.rowcount,  # number of rows affected
                "rownumber": 0,  # line number
                "query": query.replace("?", "{}").format(*parameters) if self._return_query else ""  # query executed
            }
        finally:
            # cursor.close()
            pass

    def executemany_return_detail(self, query, parameters):
        """return_detail"""
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return {
                "lastrowid": cursor.lastrowid,  # the primary key id affected
                "rowcount": cursor.rowcount,  # number of rows affected
                "rownumber": 0,  # line number
                "query": query.replace("?", "{}").format(*parameters) if self._return_query else ""  # query executed
            }
        except Exception as e:
            self._log_exception(e, query, parameters)
            self.close()
            raise
        finally:
            pass


class ChainDB(base.ChainDB):
    field_name_quote = '"'

    def connect(self, config_dict=None, return_query=False):
        config_dict["return_query"] = return_query
        self.connection = Connection(**config_dict)
        self.param_place_holder = "?"

    def parse_limit(self, sql):
        """parse limit condition"""

        if self._limit == 0:
            return sql

        # SQLite LIMIT is different from MySQL
        if self._limit:
            if isinstance(self._limit, str) and "," in self._limit:
                m, n = self._limit.replace(" ", "").split(",")
                sql += f" LIMIT {n} OFFSET {m}"
            elif self._offset:
                sql += f" LIMIT {self._limit} OFFSET {self._offset}"
            else:
                sql += f" LIMIT {self._limit} "

        return sql
