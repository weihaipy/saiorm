#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Support MySQL
"""
import ast
import time
from pymysql import cursors
from pymysql import connect

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
                 max_idle_time=7 * 3600, connect_timeout=60, autocommit=True,
                 time_zone="+0:00", charset="utf8", **kwargs):
        self.host = host
        self.database = database
        self.max_idle_time = float(max_idle_time)

        args = dict(
            host=host,
            port=int(port),
            user=user,
            passwd=password,
            db=database,
            charset=charset,
            use_unicode=True,
            init_command=('SET time_zone = "%s"' % time_zone),
            connect_timeout=connect_timeout,
            autocommit=autocommit,
            **kwargs
        )

        self.db = None
        self.db_args = args
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except Exception:
            # logging.error("Cannot connect to MySQL on %s", self.host,
            #               exc_info=True)
            pass

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "db", None) is not None:
            self.db.close()
            self.db = None

    def reconnect(self):
        """Closes the existing database connection and re-opens it.
        改用 pymysql 实现"""
        self.close()
        self.db = connect(**self.db_args)
        # self.db.autocommit(True)

    def iter(self, query, *parameters, **kwparameters):
        """Returns an iterator for the given query and parameters."""
        self._ensure_connected()
        cursor = cursors.SSCursor(self.db)
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
        if (self.db is None or
                (time.time() - self._last_use_time > self.max_idle_time)):
            self.reconnect()
        self._last_use_time = time.time()

    def _cursor(self):
        self._ensure_connected()
        return self.db.cursor()

    def _log_exception(self, exception, query, parameters):
        """log exception when execute SQL"""
        pass

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
                "query": to_unicode(cursor._executed)  # query executed
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
                "query": to_unicode(cursor._executed)  # query executed
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
                "query": to_unicode(cursor._executed)  # query executed
            }
        except Exception as e:
            self._log_exception(e, query, parameters)
            self.close()
            raise
        finally:
            cursor.close()


class ChainDB(base.ChainDB):
    def connect(self, config_dict=None):
        self.connection = Connection(**config_dict)


class PositionDB(Connection):
    """
    Implement database operation by position argument.

    Table name prefix placeholder in initialization is prefix_sign, defaults to "###".

    **Method argument position**::

    select method: table, field, condition
    insert method: table, field
    update method: table, field, condition
    delete method: table, condition
    count method: table, field, condition
    alter method: table, condition

    **Argument**
    :table: str,table name（not include prefix）
    :field: [list|tuple|str],field name.
        If call native mysql function,should use literal dict with one key-value,like:
        'username, nickname, {"reg_time": "now()"}'
    :condition: other condition,including WHERE, ORDER BY, LIMIT, GROUP BY etc.

    **Multi line**
    param same as torndb

    **return**
    insert, update, delete and their many function returns tuple with lastrowid and rowcount.

    **Usage**::

    - initialization:
        PositionDB("127.0.0.1", 3306, "database", "user", "password", "table_name_prefix)

    - call mysql function without param：
    >>>insert("user","username,nickname,{'reg_time':'now()'}", username, nickname)
    will transform to:

    .. code:: sql

        INSERT INTO user (username, nickname, reg_time) value (%s,%s,%s,now())', username, nickname

    - call mysql function with param(use %s as placeholder)：
    >>>insert('log',"user_id,{'ip': 'inet_aton(%s)'},uri,action_no",user_id,ip, uri,action_no)
    or
    >>>insert('log',"user_id,ip=inet_aton(%s),uri,action_no",user_id,ip, uri,action_no)
    will transform to

    .. code:: sql

        INSERT INTO f4isw_log (user_id,ip,uri,action_no) VALUE (%s,inet_aton(%s), %s,%s)

    - use select or count to count
    >>>select("user", "COUNT(id) AS rows_count", "")
    equals to:
    >>>count("user", "id", "")
    """

    def __init__(self, host, port, database, user=None, password=None,
                 max_idle_time=7 * 3600, connect_timeout=60, time_zone="+0:00",
                 prefix="", prefix_sign="###", grace_result=True):
        super().__init__(host, port, database, user, password,
                         max_idle_time, connect_timeout, time_zone)
        self.prefix = prefix  # table name prefix
        self.prefix_sign = prefix_sign  # 替换表前缀的字符
        self.grace_result = grace_result

    def _execute(self, cursor, query, parameters, kwparameters):
        if self.prefix_sign in query:
            query = query.replace(self.prefix_sign, self.prefix)

        super()._execute(cursor, query, parameters, kwparameters)

    def query(self, query, *parameters, **kwparameters):
        """Returns a row list for the given query and parameters."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]

            if self.grace_result:
                return [GraceDict(zip(column_names, row)) for row in cursor]
            else:
                return [zip(column_names, row) for row in cursor]
        finally:
            cursor.close()

    def execute_return_detail(self, query, *parameters, **kwparameters):
        """return lastrowid and rowcount"""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)

            return {
                "lastrowid": cursor.lastrowid,
                "rowcount": cursor.rowcount,
            }
        finally:
            cursor.close()

    def executemany_return_detail(self, query, parameters):
        """return lastrowid and rowcount"""
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return {
                "lastrowid": cursor.lastrowid,
                "rowcount": cursor.rowcount,
            }
        finally:
            cursor.close()

    def mk_insert_query(self, table, field, many=False):
        """
        :param many: bool,the sign of inserting many data in one line
        """
        table = self.prefix + table
        if isinstance(field, str):
            field = [i.strip() for i in field.split(",")]
        field_str = ""
        value_str = ""

        for i in field:
            if i.startswith("{") and i.endswith("}"):
                # native mysql function
                ei = ast.literal_eval(i)
                keys = list(ei.keys())
                values = list(ei.values())
                field_str += (keys[0] + ",")
                value_str += (values[0] + ",")
            elif "=" in i:  # native mysql function
                k, v = i.split("=")
                field_str += (k + ",")
                value_str += (v + ",")
            else:
                field_str += (i + ",")
                value_str += "%s,"
        if value_str:
            value_str = value_str[:-1]
        if not many:
            query = f"INSERT INTO {table} ({field_str[:-1]}) VALUES ({value_str})"
        else:
            query = f"INSERT INTO {table} ({field_str[:-1]}) VALUES ({value_str})"

        return query

    def mk_delete_query(self, table, condition):
        # 生成 delete 语句
        table = self.prefix + table
        query = f"DELETE FROM {table} {condition}"
        return query

    def mk_update_query(self, table, field, condition):
        # 生成 update 语句
        table = self.prefix + table
        if isinstance(field, str):
            field = [i.strip() for i in field.split(",")]

        field_str = ""
        for i in field:
            if i.startswith("{") and i.endswith("}"):
                # native mysql function
                i = ast.literal_eval(i)
                iks = list(i.keys())
                ivs = list(i.values())
                field_str += (iks[0] + "=" + str(ivs[0]) + ", ")
            elif "=" in i:  # native mysql function
                k, v = i.split("=")
                field_str += (k + "=" + v)
            else:
                field_str += (i + "=%s, ")

        query = f"UPDATE {table} SET {field_str[:-2]} {condition}"
        return query

    def insert(self, table, field, *parameters, **kwparameters):
        """
        :return: tuple,lastrowid and rowcount
        """
        query = self.mk_insert_query(table, field, many=False)
        return self.execute_return_detail(query, *parameters, **kwparameters)

    def insert_many(self, table, field, *parameters, **kwparameters):
        """
        :return: tuple,lastrowid and rowcount
        """
        query = self.mk_insert_query(table, field, many=True)
        return self.executemany_return_detail(query, *parameters, **kwparameters)

    def delete(self, table, condition, *parameters, **kwparameters):
        """
        :return: tuple,lastrowid and rowcount
        """
        query = self.mk_delete_query(table, condition)
        return self.execute_return_detail(query, *parameters, **kwparameters)

    def update(self, table, field, condition, *parameters, **kwparameters):
        """
        :return: tuple,lastrowid and rowcount
        """
        query = self.mk_update_query(table, field, condition)
        return self.execute_return_detail(query, *parameters, **kwparameters)

    def select(self, table, field, condition, *parameters, **kwparameters):
        """
        :return: list,all data with the conditions
        """
        table = self.prefix + table
        query = "SELECT " + field + " FROM " + table + " " + condition

        return self.query(query, *parameters, **kwparameters)

    def get(self, table, field, condition, *parameters, **kwparameters):
        """
        :return: dict,the latest line of data with the conditions
        """
        data = self.select(table, field, condition, *parameters, **kwparameters)
        return data[0] if data else []

    def count(self, table, field, condition, *parameters, **kwparameters):
        """
        The number of data with the conditions

        :return: int
        """
        table = self.prefix + table
        field = f'COUNT({field}) AS rows_count'
        query = f"SELECT {field} FROM {table} {condition}"
        rows_count = self.query(query, *parameters, **kwparameters)
        if rows_count:
            return int(rows_count[0]["rows_count"])
        else:
            return 0

    def increase(self, table, field, condition, *parameters, step=1, **kwparameters):
        """number field Increase """
        sql = f"UPDATE {self.prefix + table} SET {field}={field}+{step}  {condition};"
        res = self.execute_return_detail(sql, *parameters, **kwparameters)
        return res

    def decrease(self, table, field, condition, *parameters, step=1, **kwparameters):
        """number field decrease """
        sql = f"UPDATE {self.prefix + table} SET {field}={field}-{step} {condition};"
        res = self.execute_return_detail(sql, *parameters, **kwparameters)
        return res
