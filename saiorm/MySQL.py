#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
异步的支持略复杂,先不实现,只是用普通方式实现功能.

"""
import ast
import logging

try:
    from . import utility
except ImportError:
    import utility

ConnectionMySQL = utility.ConnectionMySQL
GraceDict = utility.GraceDict
Row = utility.Row
is_array = utility.is_array


class CoherentDB(object):
    """
    Implement database coherent operation.
    
    After initialization with table name,use config_db to set connected database.
    """

    def __init__(self, table_name_prefix="", debug=False, strict=True,
                 cache_fields_name=True, grace_result=True):
        self.db = None
        self.table_name_prefix = table_name_prefix
        self.debug = debug
        self.strict = strict
        self.last_sql = ""  # latest executed sql
        self.cache_fields_name = cache_fields_name  # when call get_fields_name
        self._cached_fields_name = {}  # cached fields name
        self.grace_result = grace_result

        self._table = ""
        self._where = ""
        self._order_by = ""
        self._group_by = ""
        self._limit = ""
        self._inner_join = ""
        self._left_join = ""
        self._right_join = ""
        self._on = ""

    def connect(self, config_dict=None):
        """
        set a connected torndb.Connection

        :param config_dict: dict,config to connect database
        """
        self.db = ConnectionMySQL(**config_dict)

    def execute(self, *args, **kwargs):
        """execute SQL"""
        res = self.db.execute_return_detail(*args, **kwargs)
        self._reset()  # reset param
        return res

    def executemany(self, *args, **kwargs):
        """execute SQL with many lines"""
        res = self.db.executemany_return_detail(*args, **kwargs)
        self._reset()  # reset param
        return res

    def query(self, *args, **kwargs):
        """query SQL"""
        res = self.db.query_return_detail(*args, **kwargs)
        self._reset()  # reset param
        return res

    def table(self, table_name=""):
        """
        If table_name is empty,use DB().select("now()") will run SELECT now()
        """
        # check table name prefix
        if self.table_name_prefix and not table_name.startswith(self.table_name_prefix):
            table_name += self.table_name_prefix

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

    def group_by(self, condition):
        self._group_by = condition
        return self

    def join(self, condition):
        self._inner_join = condition
        return self

    def inner_join(self, condition):
        self._inner_join = condition
        return self

    def left_join(self, condition):
        self._left_join = condition
        return self

    def right_join(self, condition):
        self._right_join = condition
        return self

    def on(self, condition):
        self._on = condition
        return self

    def select(self, fields="*"):
        """
        fields is fields or native sql function,
        ,use DB().select("now()") will run SELECT now()
        """
        condition_values = []
        if self._table and fields:
            condition_sql, condition_values = self.__parse_condition()
            sql = "SELECT {} FROM {} {};".format(fields, self._table, condition_sql)
        else:
            sql = "SELECT {};".format(fields)  # 用于直接执行 mysql 函数

        res = self.query(sql, *condition_values)
        self.last_sql = res["sql"]
        if self.grace_result:
            res["data"] = [GraceDict(i) for i in res["data"]]

        return res["data"]

    def get(self, fields="*"):
        """will replace self._limit to 1"""
        self._limit = 1
        return self.select(fields)

    def update(self, dict_data=None):
        if not dict_data:
            return
        fields, values = self.__gen_kv_str(dict_data)
        condition_sql, condition_values = self.__parse_condition()
        sql = "UPDATE {} SET {} {};".format(self._table, fields, condition_sql)
        values += condition_values
        values = tuple(values)
        res = self.execute(sql, *values)
        self.last_sql = res["sql"]
        return res

    def insert(self, dict_data=None):
        """
        insert one line,support rwo kinds data::

        1. dict with key fields and values,the values of keys are list or tuple
        respectively all field name and value

        2. dict, field name and value

        """
        if not dict_data:
            return

        keys = dict_data.keys()
        if "fields" in keys and "values" in keys:  # 字段名和值分开传
            fields = ",".join(dict_data["fields"])
            values = [v for v in dict_data["values"]]
        else:  # 普通字典
            fields = ",".join(keys)
            values = dict_data.values()

        values_sign = ",".join(["%s" for i in values])
        if fields:
            sql = "INSERT INTO {} ({}) VALUES ({});".format(self._table, fields, values_sign)
        else:
            sql = "INSERT INTO {} VALUES ({});".format(self._table, values_sign)
        values = tuple(values)
        res = self.execute(sql, *values)
        self.last_sql = res["sql"]
        return res

    def insert_many(self, dict_data=None):
        """
        insert one line,,support rwo kinds data,such as insert,
        but the values should be wraped with list or tuple

        """
        if not dict_data:
            return

        fields = ""  # 所有的字段
        values = []  # 所有的参数
        values_sign = ""  # 参数的占位符

        # 列表或元祖的结构必须一样
        if is_array(dict_data):
            dict_data_item_1 = dict_data[0]  # 应该是字典
            keys = dict_data_item_1.keys()
            fields = ",".join(keys)
            values = [tuple(i.values()) for i in dict_data]  # 字典的 values 先转换
            values_sign = ",".join(["%s" for f in keys])
        elif isinstance(dict_data, dict):  # 字段名和值分开传
            keys = dict_data.get("fields")
            if keys:  # 允许不指定字段
                fields = ",".join(dict_data["fields"])
            values = list([v for v in dict_data["values"]])  # 字典的 values 先转换
            values_sign = ",".join(["%s" for f in keys])
        else:
            logging.error("Param should be list or tuple or dict")
            return

        if fields:
            sql = "INSERT INTO {} ({}) VALUES ({});".format(self._table, fields, values_sign)
        else:
            sql = "INSERT INTO {}  VALUES ({});".format(self._table, values_sign)

        res = self.executemany(sql, values)
        self.last_sql = res["sql"]
        return res

    def delete(self):
        if self.strict and not self._where:  # 没有 where 条件禁止执行
            logging.warning("without where condition,can not delete")
            return

        condition_sql, condition_values = self.__parse_condition()
        sql = "DELETE FROM {} {};".format(self._table, condition_sql)
        res = self.execute(sql, *condition_values)
        self.last_sql = res["sql"]
        return res

    def increase(self, field, step=1):
        """number field Increase """
        sql = "UPDATE {} SET {}={}+{}".format(self._table, field, field, str(step))
        # print(sql)
        # raise
        res = self.execute(sql)
        self.last_sql = res["sql"]
        return res

    def decrease(self, field, step=1):
        """number field decrease """
        sql = "UPDATE {} SET {}={}-{}".format(self._table, field, field, str(step))
        res = self.execute(sql)
        self.last_sql = res["sql"]
        return res

    def get_fields_name(self):
        """return all fields of table"""
        if not self._table:
            return []

        if self.cache_fields_name and self._cached_fields_name.get(self._table):
            return self._cached_fields_name.get(self._table)
        else:
            res = self.db.query_return_detail("SELECT * FROM xxx LIMIT 1")
            fields_name = res["column_names"]
            self._cached_fields_name[self._table] = fields_name

            return fields_name

    # shorthand
    t = table
    w = where
    ob = order_by
    l = limit
    gb = group_by
    j = join
    ij = inner_join
    lj = left_join
    rj = right_join
    s = select
    i = insert
    im = insert_many
    u = update
    d = delete
    inc = increase
    dec = decrease

    def _reset(self):
        """reset param when call again"""
        # self._table = ""
        self._where = ""
        self._order_by = ""
        self._group_by = ""
        self._limit = ""
        self._inner_join = ""
        self._left_join = ""
        self._right_join = ""
        self._on = ""
        self.last_sql = ""  # latest executed sql

    def __parse_condition(self):
        """
        generate query condition

        **ATTENTION**

        You must check the parameters to prevent injection vulnerabilities

        """
        sql = ""
        sql_values = []
        if self._where:
            where = self._where
            if isinstance(self._where, dict):
                where = ""
                for k in self._where.keys():
                    v = self._where[k]
                    if is_array(v):
                        v_len = len(v)
                        v0 = v[0]
                        v0_entity = v0.replace(" ", "")
                        lge = ("<", "<=", ">", ">=", "!=")
                        if v0_entity in lge:  # < <= > >= !=
                            lge_index = lge.index(v0_entity)
                            sign = lge[lge_index]
                            v1 = v[1]
                            if isinstance(v1, str) and v1.startswith("`"):
                                # native mysql function starts with `,only one param
                                # join string direct
                                v1 = v1.replace("`", "")
                                if "?" in v1:
                                    v1 = v0.replace("?", v[2])
                                where += " {}{}{} AND".format(k, sign, v1)
                            else:
                                where += " {}{}%s AND".format(k, sign)
                                sql_values.append(v[1])
                        elif v_len >= 2 and v0_entity.lower() == "in":  # IN
                            # join string direct
                            v1 = v[1]
                            if v1:
                                if is_array(v1):
                                    v1 = ",".join(v1)
                                where += " {} IN ({}) AND".format(k, v1)
                        elif v0_entity.lower() == "between" and v_len == 3:  # BETWEEN
                            where += " {} BETWEEN %s AND %s AND".format(k)
                            sql_values += [v[1], v[2]]
                        elif isinstance(v0, str) and v0.startswith("`"):
                            # native mysql function starts with `,only one param
                            # join string direct
                            v0 = v0.replace("`", "")
                            if "?" in v0:
                                v0 = v0.replace("?", v[1])
                            where += " {}={} AND".format(k, v0)
                    else:
                        where += " {}={} AND".format(k, v)

            if where:
                sql += "WHERE" + where[:-3]  # trim the last AND character

        if self._on:
            if self._inner_join:
                sql += " INNER JOIN {} ON {}".format(self._inner_join, self._on)
            elif self._left_join:
                sql += " LEFT JOIN {} ON {}".format(self._left_join, self._on)
            elif self._right_join:
                sql += " RIGHT JOIN {} ON {}".format(self._right_join, self._on)

        if self._order_by:
            sql += " ORDER BY " + self._order_by

        if self._limit:
            sql += " LIMIT " + str(self._limit)

        if self._group_by:
            sql += " GROUP BY " + self._group_by

        return sql, sql_values

    def __gen_kv_str(self, dict_data):
        """
        generate str ike filed_name = %s and values,use for select and update
        :return: tuple
        """
        # todo  原生 mysql 函数的用法, 类似 where 的处理方式
        fields = ""
        values = []
        for k in dict_data.keys():
            v = dict_data[k]
            # 改用 where 的方法
            # if v.startswith("="):  # 使用 mysql 原生函数的
            #     v = v[1:]
            #     fields += k + "=" + v + ","
            # else:
            #     fields += k + ","
            fields += k + "=%s,"
            values.append(v)
        if fields:
            fields = fields[:-1]

        return fields, values


class PositionDB(ConnectionMySQL):
    """
    Implement database operation by position argument.

    把 select/insert/update/delete 这几个常用操作包装为函数，简化输入，可以方便添加表前缀。
    表前缀只需在初始化类的时候候传入 prefix_sign 即可,默认为 "###"。
    这几个常用 sql 语句拆分为表名，字段，条件三部分，最后把这三部分拼接为 torndb 所需的形式。

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
    1,call mysql function without param：
    >>>insert("user","username,nickname,{'reg_time':'now()'}", username, nickname)
    will transform to:

    .. code:: sql

        INSERT INTO user (username, nickname, reg_time) value (%s,%s,%s,now())', username, nickname

    2,call mysql function with param(use %s as placeholder)：
    >>>insert('log',"user_id,{'ip': 'inet_aton(%s)'},uri,action_no",user_id,ip, uri,action_no)
    todo 字典形式比较复杂考虑简化,使用等号,例如
    >>>insert('log',"user_id,ip=inet_aton(%s),uri,action_no",user_id,ip, uri,action_no)
    will transform to

    .. code:: sql

        INSERT INTO f4isw_log (user_id,ip,uri,action_no) VALUE (%s,inet_aton(%s), %s,%s)

    3,use select or count to count
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
        # 替换表名前缀的占位符
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

    def execute_both(self, query, *parameters, **kwparameters):
        """return lastrowid and rowcount"""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.lastrowid, cursor.rowcount
        finally:
            cursor.close()

    def executemany_both(self, query, parameters):
        """return lastrowid and rowcount"""
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
