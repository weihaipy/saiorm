#!/usr/bin/env python
# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, with_statement
import time
from pymysql import cursors
from pymysql import connect

import ast
import logging

try:
    from itertools import izip as zip  # python2,python3可直接使用zip
except:
    pass

version = "0.1"
version_info = (0, 1, 0, 0)

# todo 计划吧 Connection 改用 新的 torndb,吧新加的几个方法用新类实现一下

class Connection(object):
    """
    Bases on torndb, use pymysql for python3.
    """

    def __init__(self, host, port, database, user=None, password=None,
                 max_idle_time=7 * 3600, connect_timeout=60,
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
            **kwargs
        )

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except Exception:
            logging.error("Cannot connect to MySQL on %s", self.host,
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
        改用 pymysql 实现"""
        self.close()
        self._db = connect(**self._db_args)
        self._db.autocommit(True)

    def iter(self, query, *parameters, **kwparameters):
        """Returns an iterator for the given query and parameters."""
        self._ensure_connected()
        cursor = cursors.SSCursor(self._db)
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    def query(self, query, *parameters, **kwparameters):
        """Returns a row list for the given query and parameters."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            return [Row(zip(column_names, row)) for row in cursor]
        finally:
            cursor.close()

    def get(self, query, *parameters, **kwparameters):
        """Returns the (singular) row returned by the given query.
        If the query has no results, returns None.  If it has
        more than one result, raises an exception.
        """
        rows = self.query(query, *parameters, **kwparameters)
        if not rows:
            return None
        elif len(rows) > 1:
            raise Exception("Multiple rows returned for Database.get() query")
        else:
            return rows[0]

    # rowcount is a more reasonable default return value than lastrowid,
    # but for historical compatibility execute() must return lastrowid.
    def execute(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        return self.execute_lastrowid(query, *parameters, **kwparameters)

    def execute_lastrowid(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def execute_rowcount(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the rowcount from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.rowcount
        finally:
            cursor.close()

    def executemany(self, query, parameters):
        """Executes the given query against all the given param sequences.
        We return the lastrowid from the query.
        """
        return self.executemany_lastrowid(query, parameters)

    def executemany_lastrowid(self, query, parameters):
        """Executes the given query against all the given param sequences.
        We return the lastrowid from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def executemany_rowcount(self, query, parameters):
        """Executes the given query against all the given param sequences.
        We return the rowcount from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.rowcount
        finally:
            cursor.close()

    def query_with_detail(self, query, *parameters, **kwparameters):
        """同事返回结果和语句"""
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

    def execute_with_detail(self, query, *parameters, **kwparameters):
        # 同时返回 lastrowid  rowcount rownumber
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

    def executemany_with_detail(self, query, parameters):
        # 同时返回 lastrowid  rowcount rownumber
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

    update = delete = execute_rowcount
    updatemany = executemany_rowcount

    insert = execute_lastrowid
    insertmany = executemany_lastrowid

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
            # __PRINT_SQL 参数为非空,打印出执行的 SQL 语句
            __PRINT_SQL = kwparameters.get("__PRINT_SQL")
            if __PRINT_SQL:
                del kwparameters["__PRINT_SQL"]
                res = cursor.execute(query, kwparameters or parameters)
            else:
                res = cursor.execute(query, kwparameters or parameters)
            return res
        except Exception as e:
            logging.error("Error connecting to MySQL on %s", self.host)
            logging.error("Error query: %s", query)
            logging.error("Error parameters: %s", parameters)
            logging.error("Error kwparameters: %s", kwparameters)
            self.close()
            print("Mysql Error Info:", e)
            raise


class Row(dict):
    """A dict that allows for object-like property access syntax."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


class SequenceDB(Connection):
    """
    | **OLD CODE,NOT RECOMMENDED**
    | **老代码,不够直观,不推荐现在使用,即使可以正常运行也不推荐**

    表名,字段,条件等按顺序一次传入,能简化 SQL 语句
    把 select/insert/update/delete 这几个常用操作包装为函数，简化输入，可以方便添加表前缀。
    表前缀只需在初始化类的时候候传入 prefix_sign 即可,默认为 "###"。
    这几个常用 sql 语句拆分为表名，字段，条件三部分，最后把这三部分拼接为 torndb 所需的形式。

    **主要函数功能及其参数**::

    select 参数: table, field, condition
    insert 参数: table, field
    update 参数: table, field, condition
    delete 参数: table, condition
    count 参数: table, field, condition
    alter 参数: table, condition

    **参数**
    :table: 表名（不包含表前缀）,字符串
    :field: 字段名,列表/元组或逗号分割的字符串。
        如果执行 MySql 原声函数，需要写成字面量形式的字典，且只包含一组键值对。
        比如：'username, nickname, {"reg_time": "now()"}'
        包含三个字段,只需要传递前两个的值即可,第三个则使用 mysql 原生的 now 函数
    :condition: sql语句中其它的限制条件，比如 WHERE 、 ORDER BY 、LIMIT 、 GROUP 等

    todo 考虑把 WHERE 、 ORDER BY 、LIMIT 、 GROUP 作为单独的语句,做成连贯操作的形式
    在执行过程中处理为现在的代码能运行的形式,估计需要一个新类

    **多条操作函数**
    insert_many,update_many,delete_many的用法和单条操作的 insert, update, delete 一样,
    区别在于参数部分,参数部分是元组或列表,
    每句要使用的参数作为元组或列表放到其中,和 tornado 一致.

    **基础执行函数**
    tornado 使用 execute,execute_lastrowid,executemany,executemany_lastrowid 来处理,获得的信息单一,
    本类使用 execute_both 和 executemany_both,会同时返回 lastrowid 和 rowcount.
    insert, update 和 delete 和各自的 many 函数都使用了 execute_both 或 executemany_both.

    **用例**::
    1,无参数使用 mysql 函数：
    >>>insert("user","username,nickname,{'reg_time':'now()'}", username, nickname)
    将会被转换为：
    INSERT INTO user (username, nickname, reg_time) value (%s,%s,%s,now())', username, nickname

    2,带参数使用 mysql 函数(函数里要使用%s作为占位符)：
    >>>insert('log',"user_id,{'ip': 'inet_aton(%s)'},uri,action_no",user_id,ip, uri,action_no)
    todo 字典形式比较复杂考虑简化,使用等号
    >>>insert('log',"user_id,ip=inet_aton(%s),uri,action_no",user_id,ip, uri,action_no)
    会被转换为：
    INSERT INTO f4isw_log (user_id,ip,uri,action_no)  VALUE  (%s,inet_aton(%s), %s,%s)

    3,计数统计,可以使用 select 函数或 count 函数
    >>>select("user", "COUNT(id) AS rows_count", "")
    等同于
    >>>count("user", "id", "")

    **注意**
    调用 mysql 自身函数的时候为什么要使用字典的形式?
    如果传入的字段是字符串的话,会用逗号来分隔成列表,所以只有使用字典的形式才会避免分隔的时候出错.
    """

    def __init__(self, host, port, database, user=None, password=None,
                 max_idle_time=7 * 3600, connect_timeout=60, time_zone="+0:00",
                 prefix="", prefix_sign="###"):
        super(SequenceDB, self).__init__(host, port, database, user, password,
                                         max_idle_time, connect_timeout, time_zone)
        self.prefix = prefix  # 表前缀
        self.prefix_sign = prefix_sign  # 替换表前缀的字符

    def _execute(self, cursor, query, parameters, kwparameters):
        # 替换表名前缀的占位符
        if self.prefix_sign in query:
            query = query.replace(self.prefix_sign, self.prefix)

        super(SequenceDB, self)._execute(cursor, query, parameters, kwparameters)

    def query(self, query, *parameters, **kwparameters):
        """Returns a row list for the given query and parameters."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            return [GraceDict(zip(column_names, row)) for row in cursor]
        finally:
            cursor.close()

    # 同时返回 lastrowid 和 rowcount
    def execute_both(self, query, *parameters, **kwparameters):
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.lastrowid, cursor.rowcount
        finally:
            cursor.close()

    def executemany_both(self, query, parameters):
        # 同时返回 lastrowid 和 rowcount
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.lastrowid, cursor.rowcount
        finally:
            cursor.close()

    def mk_insert_query(self, table, field, many=False):
        # 生成 insert 语句,many 表示是否是多条插入
        table = self.prefix + table
        if isinstance(field, str):  # 去除空格后，分割为列表
            field = [i.strip() for i in field.split(",")]
        field_str = " ("  # 字段名部分
        field_value_str = " ("  # 字段名对应的参数部分

        for i in field:  # 判断是否有使用 mysql 自身函数的情况
            if i.startswith("{") and i.endswith("}"):
                ei = ast.literal_eval(i)
                keys = list(ei.keys())
                values = list(ei.values())
                field_str += (keys[0] + ",")
                field_value_str += (values[0] + ",")
            else:
                field_str += (i + ",")
                field_value_str += "%s,"
        field_str = field_str[:-1] + ") "
        field_value_str = field_value_str[:-1] + ") "
        if not many:
            query = "INSERT INTO " + table + field_str + " VALUE " + field_value_str
        else:
            query = "INSERT INTO " + table + field_str + " VALUES " + field_value_str
        return query

    def mk_delete_query(self, table, condition):
        # 生成 delete 语句
        table = self.prefix + table
        query = "DELETE FROM " + table + " " + condition
        return query

    def mk_update_query(self, table, field, condition):
        # 生成 update 语句
        table = self.prefix + table
        if isinstance(field, str):  # 去除空格后，分割为列表
            field = [i.strip() for i in field.split(",")]
        field_str = " "
        for i in field:  # 判断是否有使用 mysql 自身函数的情况
            if i.startswith("{") and i.endswith("}"):
                i = ast.literal_eval(i)
                iks = list(i.keys())
                ivs = list(i.values())

                field_str += (iks[0] + "=" + str(ivs[0]) + ", ")
            else:
                field_str += (i + "=%s, ")

        field_str = field_str[:-2] + " "
        query = "UPDATE " + table + " SET" + field_str + " " + condition
        return query

    def insert(self, table, field, *parameters, **kwparameters):
        query = self.mk_insert_query(table, field, many=True)
        return self.execute_both(query, *parameters, **kwparameters)

    def insert_many(self, table, field, *parameters, **kwparameters):
        query = self.mk_insert_query(table, field, many=True)
        return self.executemany_both(query, *parameters, **kwparameters)

    def delete(self, table, condition, *parameters, **kwparameters):
        query = self.mk_delete_query(table, condition)
        return self.execute_both(query, *parameters, **kwparameters)

    def delete_many(self, table, condition, *parameters, **kwparameters):
        query = self.mk_delete_query(table, condition)
        return self.executemany_both(query, *parameters, **kwparameters)

    def update(self, table, field, condition, *parameters, **kwparameters):
        query = self.mk_update_query(table, field, condition)
        return self.execute_both(query, *parameters, **kwparameters)

    def update_many(self, table, field, condition, *parameters, **kwparameters):
        query = self.mk_update_query(table, field, condition)
        return self.executemany_both(query, *parameters, **kwparameters)

    def select(self, table, field, condition, *parameters, **kwparameters):
        table = self.prefix + table
        query = "SELECT " + field + " FROM " + table + " " + condition
        return self.query(query, *parameters, **kwparameters)

    def get(self, table, field, condition, *parameters, **kwparameters):
        # 只返回select的第一条数据
        data = self.select(table, field, condition, *parameters, **kwparameters)
        return data[0] if data else []

    def count(self, table, field, condition, *parameters, **kwparameters):
        # 用于计数,和select类似，简化了一些输入
        table = self.prefix + table
        field = 'COUNT(' + field + ') AS rows_count'
        query = "SELECT " + field + " FROM " + table + " " + condition
        rows_count = self.query(query, *parameters, **kwparameters)
        if rows_count:
            return int(rows_count[0]["rows_count"])
        else:
            return 0

    def alter(self, table, condition, *parameters, **kwparameters):
        query = "ALTER TABLE " + self.prefix + table + " " + condition
        return self.execute_lastrowid(query, *parameters, **kwparameters)


class GraceDict(dict):
    """更优雅的字典,没有键就返回空字符串,而不是抛出 KeyError"""

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
