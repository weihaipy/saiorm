#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Support PostgreSQL

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


class Connection(object):
    def __init__(self, host, return_query=False):
        self.host = host
        self._return_query = return_query

        self._db = None
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except Exception:
            logging.error("Cannot connect to SQLite on {}".format(self.host),
                          exc_info=True)

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.close()

        self._db = sqlite3.connect(self.host)

    def iter(self, query, *parameters, **kwparameters):
        """Returns an iterator for the given query and parameters."""
        # self._ensure_connected()
        # cursor = cursors.SSCursor(self._db) # psycopg2 没有 cursors
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    #
    # def _ensure_connected(self):
    #     # Mysql by default closes client connections that are idle for
    #     # 8 hours, but the client library does not report this fact until
    #     # you try to perform a query and it fails.  Protect against this
    #     # case by preemptively closing and reopening the connection
    #     # if it has been idle for too long (7 hours by default).
    #     if (self._db is None or
    #             (time.time() - self._last_use_time > self.max_idle_time)):
    #         self.reconnect()
    #     self._last_use_time = time.time()

    def _cursor(self):
        return self._db.cursor()

    def _log_exception(self, exception, query, parameters):
        """log exception when execute SQL"""
        pass
        # logging.error("Error on SQLite:" + self.host)
        # logging.error("Error query:", query.replace("%s", "{}").format(*parameters))
        # logging.error("Error Exception:" + str(exception))

    def _execute(self, cursor, query, parameters, kwparameters):
        try:
            res = cursor.execute(query, kwparameters or parameters)
            # TODO check auto commit
            return res
        except Exception as e:
            self._log_exception(e, query, parameters)
            self.close()
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
            # cursor.close()
            pass


class ChainDB(base.ChainDB):
    def connect(self, config_dict=None, return_query=False):
        config_dict["return_query"] = return_query
        self.db = Connection(**config_dict)
        self.param_place_holder = "?"

    def parse_condition(self):
        """
        generate query condition

        **ATTENTION**

        You must check the parameters to prevent injection vulnerabilities

        """
        sql, sql_values = self.parse_where_condition()
        if self._inner_join:
            sql += " INNER JOIN {} ON {}".format(self._inner_join, self._on)
        elif self._left_join:
            sql += " LEFT JOIN {} ON {}".format(self._left_join, self._on)
        elif self._right_join:
            sql += " RIGHT JOIN {} ON {}".format(self._right_join, self._on)

        if self._order_by:
            sql += " ORDER BY " + self._order_by

        # PostgreSQL LIMIT id different from MySQL
        if self._limit:
            if isinstance(self._limit, str) and "," in self._limit:
                m, n = self._limit.replace(" ", "").split(",")
                sql += " LIMIT {} OFFSET {}".format(n, m)
            else:
                sql += " LIMIT " + str(self._limit)

        if self._group_by:
            sql += " GROUP BY " + self._group_by

        return sql, sql_values
