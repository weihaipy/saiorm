#!/usr/bin/env python
# -*- coding:utf-8 -*-
import time
import logging

try:
    from . import utility
except ImportError:
    import utility

GraceDict = utility.GraceDict
is_array = utility.is_array
Row = utility.Row
to_unicode = utility.to_unicode


class BaseConnection(object):
    """default MySQL"""
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
            pass

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "db", None) is not None:
            self.db.close()
            self.db = None

    def reconnect(self):
        raise NotImplementedError("You must implement it in subclass")

    def iter(self, query, *parameters, **kwparameters):
        raise NotImplementedError("You must implement it in subclass")

    def _log_exception(self, exception, query, parameters):
        raise NotImplementedError("You must implement it in subclass")

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


class BaseDB(object):
    """
    Implement database chain  operation.

    After initialization with table name,use config_db to set connected database.

    In JOIN,use ### as table name prefix placeholder.

    If use SQL Server, param primary_key is necessary,used in the LIMIT implement tec.

    """

    def __init__(self, table_name_prefix="", debug=False, strict=True,
                 cache_fields_name=True, grace_result=True):
        self.connection = None
        self.table_name_prefix = table_name_prefix
        self.debug = debug
        self.strict = strict

        self.cache_fields_name = cache_fields_name  # when call get_fields_name
        self._cached_fields_name = {}  # cached fields name
        self.grace_result = grace_result
        self.param_place_holder = "%s"  # SQLite will use ?
        self.field_name_quote = "`"  # MySQL use `,PostgreSql and SQLite use ",SQLServer use ', new in 0.2

        self._table = ""
        self._reset()

    def _reset(self):
        """reset param when call again"""
        # self._table = "" # keep table name
        self._where = ""
        self._order_by = ""
        self._group_by = ""
        self._limit = 0
        self._offset = 0
        self._inner_join = ""
        self._left_join = ""
        self._right_join = ""
        self._outer_join = ""
        self._full_join = ""
        self._on = ""
        self.last_query = ""  # latest executed sql

    def wrap_field_name(self, fields):
        """
        wrap each field name with quote. should used in SELECT statement
        new in 0.2
        """
        # todo  在 join 的时候,添加符号可能导致出错,需要判断是否有点,然后分开处理
        if self.field_name_quote not in fields and "," in fields:
            if "." not in fields:
                fields = self.field_name_quote + \
                         self.field_name_quote.join(fields.split(",")) + \
                         self.field_name_quote
        return fields

    def connect(self, config_dict=None):
        """
        set a connected torndb.Connection

        :param config_dict: dict,config to connect database
        """
        raise NotImplementedError("You must implement it in subclass")

    def execute(self, *args, **kwargs):
        """execute SQL"""
        res = self.connection.execute_return_detail(*args, **kwargs)
        self._reset()  # reset param
        return res

    def executemany(self, *args, **kwargs):
        """execute SQL with many lines"""
        res = self.connection.executemany_return_detail(*args, **kwargs)
        self._reset()  # reset param
        return res

    def query(self, *args, **kwargs):
        """query SQL"""
        res = self.connection.query_return_detail(*args, **kwargs)
        self._reset()  # reset param
        return res

    def table(self, table_name="", *args):
        """
        If table_name is empty,use DB().select("now()") will run SELECT now()
        """
        # check table name prefix
        if self.table_name_prefix and not table_name.startswith(self.table_name_prefix):
            table_name = self.table_name_prefix + table_name
        self._table = table_name
        return self

    def where(self, condition):
        self._where = condition
        return self

    def order_by(self, condition):
        self._order_by = condition
        return self

    def limit(self, condition):
        self._limit = condition
        return self

    def offset(self, condition):
        self._offset = condition
        return self

    def group_by(self, condition):
        self._group_by = condition
        return self

    def on(self, condition):
        if self.table_name_prefix and "###" in condition:
            condition = condition.replace("###", self.table_name_prefix)
        self._on = condition
        return self

    def join(self, condition):
        if self.table_name_prefix:
            if "###" in condition:
                condition = condition.replace("###", self.table_name_prefix)
            else:
                condition = self.table_name_prefix + condition
        self._inner_join = condition
        return self

    def inner_join(self, condition):
        if self.table_name_prefix:
            if "###" in condition:
                condition = condition.replace("###", self.table_name_prefix)
            else:
                condition = self.table_name_prefix + condition
        self._inner_join = condition
        return self

    def left_join(self, condition):
        if self.table_name_prefix:
            if "###" in condition:
                condition = condition.replace("###", self.table_name_prefix)
            else:
                condition = self.table_name_prefix + condition
        self._left_join = condition
        return self

    def right_join(self, condition):
        if self.table_name_prefix:
            if "###" in condition:
                condition = condition.replace("###", self.table_name_prefix)
            else:
                condition = self.table_name_prefix + condition
        self._right_join = condition
        return self

    def select(self, fields="*"):
        """
        fields is fields or native sql function,
        ,use DB().select("=now()") will run SELECT now()
        """
        condition_values = []
        if fields.startswith("`"):  # native function
            sql = self.gen_select_without_fields(fields[1:])  # 用于直接执行 mysql 函数
        else:
            condition_sql, condition_values = self.parse_condition()
            sql = self.gen_select_with_fields(fields, condition_sql)
        res = self.query(sql, *condition_values)
        self.last_query = res["query"]
        if self.grace_result:
            res["data"] = [GraceDict(i) for i in res["data"]]

        return res["data"]

    def gen_select_with_fields(self, fields, condition):
        raise NotImplementedError("You must implement it in subclass")

    def gen_select_without_fields(self, fields):
        raise NotImplementedError("You must implement it in subclass")

    def get(self, fields="*"):
        """will replace self._limit to 1"""
        self._limit = 1
        res = self.select(fields)
        return res[0] if res else {}  # return dit type

    def update(self, dict_data=None):
        if not dict_data:
            return False
        fields, values = self.split_update_fields_value(dict_data)
        condition_sql, condition_values = self.parse_condition()
        sql = self.gen_update(fields, condition_sql)
        values += condition_values
        values = tuple(values)
        res = self.execute(sql, *values)
        self.last_query = res["query"]
        return res

    def gen_update(self, *args, **kwargs):
        raise NotImplementedError("You must implement it in subclass")

    def split_update_fields_value(self, *args, **kwargs):
        raise NotImplementedError("You must implement it in subclass")

    def insert(self, dict_data=None):
        """
        insert one line,support rwo kinds data::

        1. dict with key fields and values,the values of keys are list or tuple
        respectively all field name and value

        2. dict, field name and value

        """
        if not dict_data:
            return False

        keys = dict_data.keys()
        if "fields" in keys and "values" in keys:  # split dict
            fields = ",".join(dict_data["fields"])
            values = [v for v in dict_data["values"]]
        elif "values" in keys and len(keys) == 1:  # split dict without fields
            fields = None
            values = [v for v in dict_data["values"]]
        else:  # natural dict
            fields = ",".join(keys)
            values = dict_data.values()

        values_sign = ",".join([self.param_place_holder for i in values])
        if fields:
            sql = self.gen_insert_with_fields(fields, values_sign)
        else:
            sql = self.gen_insert_without_fields(values_sign)

        res = self.execute(sql, *values)
        self.last_query = res["query"]
        return res

    def gen_insert_with_fields(self, *args, **kwargs):
        raise NotImplementedError("You must implement it in subclass")

    def gen_insert_without_fields(self, *args, **kwargs):
        raise NotImplementedError("You must implement it in subclass")

    def insert_many(self, dict_data=None):
        """
        insert one line,,support rwo kinds data,such as insert,
        but the values should be wraped with list or tuple

        """
        if not dict_data:
            return False

        fields = ""  # all fields

        if is_array(dict_data):
            dict_data_item_1 = dict_data[0]  # should be dict
            keys = dict_data_item_1.keys()
            fields = ",".join(keys)
            values = [tuple(i.values()) for i in dict_data]
            values_sign = ",".join([self.param_place_holder for f in keys])
        elif isinstance(dict_data, dict):  # split dict
            keys = dict_data.get("fields")
            if keys:
                if "values" in keys and len(keys) == 1:  # split dict without fields
                    fields = None
                else:
                    fields = ",".join(dict_data["fields"])
            values = list([v for v in dict_data["values"]])
            values_sign = ",".join([self.param_place_holder for f in keys])
        else:
            logging.error("Param should be list or tuple or dict")
            return False

        if fields:
            sql = self.gen_insert_with_fields(fields, values_sign)
        else:
            sql = self.gen_insert_without_fields(values_sign)

        values = tuple([tuple(i) for i in values])  # SQL Server support tuple only

        res = self.executemany(sql, values)
        self.last_query = res["query"]
        return res

    def gen_insert_many_with_fields(self, *args, **kwargs):
        raise NotImplementedError("You must implement it in subclass")

    def gen_insert_many_without_fields(self, *args, **kwargs):
        raise NotImplementedError("You must implement it in subclass")

    def delete(self):
        if self.strict and not self._where:
            logging.warning("without where condition,can not delete")
            return False

        sql, sql_values = self.gen_delete()
        res = self.execute(sql, *sql_values)
        self.last_query = res["query"]
        return res

    def gen_delete(self, *args, **kwargs):
        raise NotImplementedError("You must implement it in subclass")

    def increase(self, field, step=1):
        """number field Increase"""
        sql, sql_values = self.gen_increase(field, str(step))
        res = self.execute(sql, sql_values)
        self.last_query = res["query"]
        return res

    def gen_increase(self, *args, **kwargs):
        raise NotImplementedError("You must implement it in subclass")

    def decrease(self, field, step=1):
        """number field decrease """
        sql, sql_values = self.gen_decrease(field, str(step))
        res = self.execute(sql, *sql_values)
        self.last_query = res["query"]
        return res

    def gen_decrease(self, *args, **kwargs):
        raise NotImplementedError("You must implement it in subclass")

    def get_fields_name(self, *args, **kwargs):
        """return all fields of table"""
        if not self._table:
            return []

        if self.cache_fields_name and self._cached_fields_name.get(self._table):
            return self._cached_fields_name.get(self._table)
        else:
            res = self.connection.query_return_detail(self.gen_get_fields_name())
            fields_name = res["column_names"]
            self._cached_fields_name[self._table] = fields_name

            return fields_name

    def gen_get_fields_name(self, *args, **kwargs):
        """get one line from table"""
        raise NotImplementedError("You must implement it in subclass")

    def parse_where_condition(self, *args, **kwargs):
        """
        parse where condition

        where condition is the most complex condition,let it alone
        """
        raise NotImplementedError("You must implement it in subclass")

    def parse_condition(self, *args, **kwargs):
        """
        parse all condition

        Calling parse_where_condition first is a good idea.
        """
        raise NotImplementedError("You must implement it in subclass")

    def begin(self, *args, **kwargs):
        """
        Transaction
        """
        raise NotImplementedError("You must implement it in subclass")

    def commit(self, *args, **kwargs):
        """
        Transaction
        """
        raise NotImplementedError("You must implement it in subclass")

    def rollback(self, *args, **kwargs):
        """
        Transaction
        """
        raise NotImplementedError("You must implement it in subclass")

    fetchall = select  # alias
    fetchone = get  # alias


class ChainDB(BaseDB):
    """
    Common SQL class,Most basic SQL statements are same as each other.

    Statements are MySQL style.For other type ,implement the difference only.
    """

    def gen_select_with_fields(self, fields, condition):
        fields = self.wrap_field_name(fields)
        return f"SELECT {fields} FROM {self._table} {condition};"

    def gen_select_without_fields(self, fields):
        return f"SELECT {fields};"

    def split_update_fields_value(self, dict_data):
        """
        generate str ike filed_name = %s and values,use for update
        :return: tuple
        """
        fields = ""
        values = []
        for k in dict_data.keys():
            v = dict_data[k]

            if isinstance(v, str):
                if v.startswith("`"):  # native function without param
                    v = v[1:]
                    fields += f"{k}={v},"
                else:
                    fields += k + "=" + self.param_place_holder + ","
                    values.append(v)
            elif is_array(v):  # native function with param
                v0 = v[0]
                if v0.startswith("`"):
                    v0 = v0[1:]
                v0 = v0.replace("?", self.param_place_holder)
                fields += f"{k}={v},"
                values.append(v[1])

        if fields:
            fields = fields[:-1]

        return fields, values

    def gen_update(self, fields, condition):
        return f"UPDATE {self._table} SET {fields} {condition};"

    def gen_insert_with_fields(self, fields, values_sign):
        return f"INSERT INTO {self._table} ({fields}) VALUES ({values_sign});"

    def gen_insert_without_fields(self, values_sign):
        return f"INSERT INTO {self._table} VALUES ({values_sign});"

    def gen_insert_many_with_fields(self, fields, values_sign):
        return f"INSERT INTO {self._table} ({fields}) VALUES ({values_sign});"

    def gen_insert_many_without_fields(self, values_sign):
        return f"INSERT INTO {self._table}  VALUES ({values_sign});"

    def gen_delete(self):
        sql_where, sql_values_where = self.parse_where_condition("")
        return f"DELETE FROM {self._table} {sql_where};", sql_values_where

    def gen_increase(self, field, step):
        """number field Increase"""
        sql_where, sql_values_where = self.parse_where_condition("")
        return f"UPDATE {self._table} SET {field}={field}+{step} {sql_where};", sql_values_where

    def gen_decrease(self, field, step=1):
        """number field decrease"""
        sql_where, sql_values_where = self.parse_where_condition("")
        return f"UPDATE {self._table} SET {field}={field}-{step} {sql_where};", sql_values_where

    def gen_get_fields_name(self):
        """get one line from table"""
        return f"SELECT * FROM {self._table} LIMIT 1;"

    def parse_join(self):
        """parse join condition"""
        sql = ""
        if self._inner_join:
            sql += f" INNER JOIN {self._inner_join} ON {self._on}"
        elif self._left_join:
            sql += f" LEFT JOIN {self._left_join} ON {self._on}"
        elif self._right_join:
            sql += f" RIGHT JOIN {self._right_join} ON {self._on}"
        elif self._outer_join:
            sql += f" OUTER JOIN {self._right_join} ON {self._on}"
        elif self._full_join:
            sql += f" FULL JOIN {self._right_join} ON {self._on}"
        return sql

    def parse_where_condition(self, sql):
        """parse where condition
        :param sql:str
        """
        sql = sql + " "  # Add a space
        sql_values = []
        if self._where:
            if isinstance(self._where, str):
                sql += "WHERE" + self._where
            elif isinstance(self._where, list) or isinstance(self._where, tuple):
                where = ""
                and_or_length = 3  # length of AND or OR
                for i in self._where:  # WHERE SHOULD BE LIST, change in 0.2
                    k = i[0]  # field name
                    v = i[1:]  # where value
                    v0 = v[0]
                    if len(v) > 1:
                        and_or = "AND"  # Parallel relationship
                        if isinstance(v0, str) and v0.lower() == "or":
                            and_or = "OR"
                            and_or_length = 2
                            v = v[1:]
                            if len(v) == 1:
                                v = v[0]
                                v0 = None
                            else:
                                v0 = v[0]

                        sign = v0.strip() if isinstance(v0, str) else ""

                        if sign and sign[0] in ("<", ">", "!"):  # < <= > >= !=
                            v1 = v[1]
                            if isinstance(v1, str) and v1.startswith("`"):
                                # native mysql function starts with `
                                # JOIN STRING DIRECT
                                v1 = v1.replace("`", "")
                                if "?" in v1:
                                    v0 = v0.replace("?", "{}")
                                    v = v0.format(*v[1:])
                                where += f" {k}{sign}{v} {and_or}"
                            else:
                                where += f" {k}{sign}{self.param_place_holder} {and_or}"
                                sql_values.append(v[1])
                        elif sign.lower() in ("in", "not in", "is not"):
                            # IN / NOT IN / IS NOT etc.
                            # JOIN STRING DIRECT
                            v1 = v[1]

                            if is_array(v1):
                                v1 = ",".join(v1)
                                where += f" {k} {sign} ({v1}) {and_or}"
                            else:
                                where += f" {k} {sign} {v1} {and_or}"
                        elif sign.lower() == "between":  # BETWEEN
                            where += f" {k} BETWEEN {self.param_place_holder} AND {self.param_place_holder} {and_or}"
                            sql_values += [v[1], v[2]]
                        elif sign.startswith("`"):  # native mysql function
                            # JOIN STRING DIRECT
                            v0 = v0.replace("`", "")
                            if "?" in v0:
                                v0 = v0.replace("?", "{}")
                                v0 = v0.format(*v[1:])
                            where += f" {k}={v0} {and_or}"
                        else:
                            if isinstance(v, str) and v.startswith("`"):  # native mysql function
                                where += f" {k}={v[1:]} {and_or}"
                            else:
                                where += f" {k}={self.param_place_holder} {and_or}"
                                sql_values.append(v)
                    else:  # single value
                        and_or_length = 3
                        if isinstance(v0, str) and v0.startswith("`"):  # native mysql function
                            where += f" {k}={v[1:]} AND"
                        else:
                            where += f" {k}={self.param_place_holder} AND"
                            sql_values.append(v0)
                if where:
                    sql += "WHERE" + where[:0 - and_or_length]  # trim the last AND / OR character

        return sql, sql_values

    def parse_order_by(self, sql):
        """parse order by condition"""
        if self._order_by:
            sql += f" ORDER BY {self._order_by} "
        return sql

    def parse_limit(self, sql):
        """parse limit condition"""

        if self._limit == 0:
            return sql

        if isinstance(self._limit, str) and "," in self._limit:
            m, n = self._limit.replace(" ", "").split(",")
            sql += f" LIMIT {m} {n}"
        elif self._offset:
            sql += f" LIMIT {self._offset}, {self._limit}"
        else:
            sql += f" LIMIT {self._limit} "
        return sql

    def parse_group_by(self, sql):
        """parse order by condition"""
        if self._group_by:
            sql += " GROUP BY {} ".format(self._group_by)
        return sql

    def parse_condition(self):
        """
        generate query condition
        """
        sql = ""
        # should parse join statement,the next one is whee statement
        if any((self._inner_join, self.left_join, self.right_join)):
            sql = self.parse_join()
        sql, sql_values = self.parse_where_condition(sql)
        sql = self.parse_order_by(sql)
        sql = self.parse_limit(sql)
        sql = self.parse_group_by(sql)

        return sql, sql_values

    def begin(self, *args, **kwargs):
        """
        Transaction
        """
        # self.connection.db.autocommit(False)
        # todo pymysql 可能需要重新初始化才能修改 autocommit

        self.execute("START TRANSACTION;")

    def commit(self, *args, **kwargs):
        """
        Transaction
        """
        self.execute("COMMIT;")

    def rollback(self, *args, **kwargs):
        """
        Transaction
        """
        self.execute("ROLLBACK;")
        self.connection.db.autocommit(True)
