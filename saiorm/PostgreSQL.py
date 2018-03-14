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


class ConnectionPostgreSQL(object):
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

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except Exception:
            logging.error("Cannot connect to PostgreSQL on {}:{}".format(self.host, port),
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

        self._db = psycopg2.connect(**self._db_args)
        self._db.set_session(autocommit=True)  # psycopg2 的设置方法不一样

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

    def _log_exception(self, exception, query, parameters):
        """log exception when execute SQL"""
        logging.error("Error on postgresSQL:" + self.host)
        logging.error("Error query:", query.replace("%s", "{}").format(*parameters))
        logging.error("Error Exception:" + str(exception))

    def _execute(self, cursor, query, parameters, kwparameters):
        try:
            return cursor.execute(query, kwparameters or parameters)
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
    def connect(self, config_dict=None):
        self.db = ConnectionPostgreSQL(**config_dict)

    def parse_condition(self):
        """
        generate query condition

        **ATTENTION**

        You must check the parameters to prevent injection vulnerabilities

        """
        sql = ""
        sql_values = []
        if self._where:
            if isinstance(self._where, str):
                sql += "WHERE" + self._where
            elif isinstance(self._where, dict):
                where = ""
                for k in self._where.keys():
                    v = self._where[k]
                    if is_array(v):
                        v0 = v[0]
                        sign = v0.strip()

                        if v0[0] in ("<", ">", "!"):  # < <= > >= !=
                            v1 = v[1]
                            if isinstance(v1, str) and v1.startswith("`"):
                                # native mysql function starts with `
                                # JOIN STRING DIRECT
                                v1 = v1.replace("`", "")
                                if "?" in v1:
                                    v0 = v0.replace("?", "{}")
                                    v = v0.format(*v[1:])
                                where += " {}{}{} AND".format(k, sign, v)
                            else:
                                where += " {}{}%s AND".format(k, sign)
                                sql_values.append(v[1])
                        elif sign.lower() == "in":  # IN
                            # JOIN STRING DIRECT
                            v1 = v[1]
                            if v1:
                                if is_array(v1):
                                    v1 = ",".join(v1)
                                where += " {} IN ({}) AND".format(k, v1)
                        elif sign.lower() == "between":  # BETWEEN
                            where += " {} BETWEEN %s AND %s AND".format(k)
                            sql_values += [v[1], v[2]]
                        elif sign.startswith("`"):
                            # native mysql function starts with `
                            # JOIN STRING DIRECT
                            v0 = v0.replace("`", "")
                            if "?" in v0:
                                v0 = v0.replace("?", "{}")
                                v0 = v0.format(*v[1:])
                            where += " {}={} AND".format(k, v0)
                    else:
                        if isinstance(v, str) and v.startswith("`"):
                            # native mysql function
                            where += " {}={} AND".format(k, v[1:])
                        else:
                            where += " {}=%s AND".format(k)
                            sql_values.append(v)
                if where:
                    sql += "WHERE" + where[:-3]  # trim the last AND character

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
