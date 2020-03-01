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


class Connection(object):
    def __init__(self, host, port, database, user=None, password=None,
                 max_idle_time=7 * 3600, return_query=False):
        self.host = host
        self.database = database
        self.max_idle_time = float(max_idle_time)
        self._return_query = return_query

        args = dict(
            host=host,
            port=str(port),
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

    def _log_exception(self, exception, query, parameters):
        """log exception when execute SQL"""
        pass
        # logging.info("Error on SQL Server:" + self.host)
        # logging.info("Error query:", query.replace("%s", "{}").format(*parameters))
        # logging.info("Error Exception:" + str(exception))

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
                "data": [Row(zip(column_names, row)) for row in cursor.fetchall()],
                "column_names": column_names,
                "query": query.replace("%s", "{}").format(*parameters) if self._return_query else ""  # query executed
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
                "query": query.replace("%s", "{}").format(*parameters) if self._return_query else ""  # query executed
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
                "query": query.replace("%s", "{}").format(*parameters) if self._return_query else ""  # query executed
            }
        except Exception as e:
            self._log_exception(e, query, parameters)
            self.close()
            raise
        finally:
            cursor.close()


class ChainDB(base.ChainDB):
    def __init__(self, table_name_prefix="", debug=False, strict=True,
                 cache_fields_name=True, grace_result=True, primary_key=""):
        self._primary_key = primary_key  # For SQL Server
        self._return_query = None
        self.field_name_quote = "'"
        super().__init__(table_name_prefix=table_name_prefix, debug=debug, strict=strict,
                         cache_fields_name=cache_fields_name, grace_result=grace_result)

    def connect(self, config_dict=None, return_query=False):
        config_dict["return_query"] = return_query
        self.db = Connection(**config_dict)

    def table(self, table_name="", primary_key=""):
        """
        If table_name is empty,use DB().select("now()") will run SELECT now()
        """
        self._primary_key = primary_key
        super().table(table_name=table_name)
        return self

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

                # used here:
                # SELECT TOP (n-m+1) id FROM tablename
                # WHERE id NOT IN (
                #  SELECT TOP m-1 id FROM tablename
                # )
                # another way :
                # SELECT * FROM xxx ORDER BY id OFFSET 5 ROWS FETCH NEXT 5 ROWS ONLY

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
                        pre_where = " WHERE {pk} NOT IN (SELECT TOP {m}-1 {pk} FROM {table}) ".format(param)
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
                # todo after fixing join statement,here will have issue
                if condition_sql.startswith("WHERE"):
                    condition_sql = pre_where + " AND " + condition_sql[len("WHERE"):]
                else:
                    condition_sql = pre_where + " AND " + condition_sql

            sql = pre_sql + condition_sql

        res = self.query(sql, *condition_values)
        self.last_query = res["query"]
        if self.grace_result:
            res["data"] = [GraceDict(i) for i in res["data"]]

        return res["data"]

    def gen_get_fields_name(self):
        """get one line from table"""
        return "SELECT TOP 1 * FROM {};".format(self._table)
