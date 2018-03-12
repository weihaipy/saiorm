#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Support MySQL
"""
import ast

try:
    from . import utility
except ImportError:
    import utility

try:
    from . import base
except ImportError:
    import base

try:
    import x_torndb
except ImportError:
    from . import x_torndb

torndb = x_torndb
Row = torndb.Row
GraceDict = utility.GraceDict
is_array = utility.is_array


class ConnectionMySQL(torndb.Connection):
    def query_return_detail(self, query, *parameters, **kwparameters):
        """return_detail"""
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

    def execute_return_detail(self, query, *parameters, **kwparameters):
        """return_detail"""
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

    def executemany_return_detail(self, query, parameters):
        """return_detail"""
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


class ChainDB(base.BaseDB):
    def connect(self, config_dict=None):
        self.db = ConnectionMySQL(**config_dict)

    def gen_select_with_fields(self, fields, condition):
        return "SELECT {} FROM {} {};".format(fields, self._table, condition)

    def gen_select_without_fields(self, fields):
        return "SELECT {};".format(fields)

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
                    fields += "{}={},".format(k, v)
                else:
                    fields += k + "=%s,"
                    values.append(v)
            elif is_array(v):  # native function with param
                v0 = v[0]
                if v0.startswith("`"):
                    v0 = v0[1:]
                v0 = v0.replace("?", "%s")
                fields += "{}={},".format(k, v0)
                values.append(v[1])

        if fields:
            fields = fields[:-1]

        return fields, values

    def gen_update(self, fields, condition):
        return "UPDATE {} SET {} {};".format(self._table, fields, condition)

    def gen_insert_with_fields(self, fields, values_sign):
        return "INSERT INTO {} ({}) VALUES ({});".format(self._table, fields, values_sign)

    def gen_insert_without_fields(self, values_sign):
        return "INSERT INTO {} VALUES ({});".format(self._table, values_sign)

    def gen_insert_many_with_fields(self, fields, values_sign):
        return "INSERT INTO {} ({}) VALUES ({});".format(self._table, fields, values_sign)

    def gen_insert_many_without_fields(self, values_sign):
        return "INSERT INTO {}  VALUES ({});".format(self._table, values_sign)

    def gen_delete(self, condition):
        return "DELETE FROM {} {};".format(self._table, condition)

    def gen_increase(self, field, step):
        """number field Increase """
        return "UPDATE {} SET {}={}+{};".format(self._table, field, field, step)

    def gen_decrease(self, field, step=1):
        """number field decrease """
        return "UPDATE {} SET {}={}-{};".format(self._table, field, field, str(step))

    def gen_get_fields_name(self):
        """get one line from table"""
        return "SELECT * FROM {} LIMIT 1;".format(self._table)

    def parse_condition(self):
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

        if self._limit:
            sql += " LIMIT " + str(self._limit)

        if self._group_by:
            sql += " GROUP BY " + self._group_by

        return sql, sql_values


class PositionDB(ConnectionMySQL):
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
        if not many:
            query = "INSERT INTO {} ({}) VALUE {}".format(table, field_str[:-1], value_str)
        else:
            query = "INSERT INTO {} ({}) VALUES {}".format(table, field_str[:-1], value_str)
        return query

    def mk_delete_query(self, table, condition):
        # 生成 delete 语句
        table = self.prefix + table
        query = "DELETE FROM " + table + " " + condition
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

        query = "UPDATE {} SET {} {}".format(table, field_str[:-2], condition)
        return query

    def insert(self, table, field, *parameters, **kwparameters):
        """

        :return: tuple,lastrowid and rowcount
        """
        query = self.mk_insert_query(table, field, many=True)
        return self.execute_both(query, *parameters, **kwparameters)

    def insert_many(self, table, field, *parameters, **kwparameters):
        """

        :return: tuple,lastrowid and rowcount
        """
        query = self.mk_insert_query(table, field, many=True)
        return self.executemany_both(query, *parameters, **kwparameters)

    def delete(self, table, condition, *parameters, **kwparameters):
        """

        :return: tuple,lastrowid and rowcount
        """
        query = self.mk_delete_query(table, condition)
        return self.execute_both(query, *parameters, **kwparameters)

    def update(self, table, field, condition, *parameters, **kwparameters):
        """
        
        :return: tuple,lastrowid and rowcount
        """
        query = self.mk_update_query(table, field, condition)
        return self.execute_both(query, *parameters, **kwparameters)

    def select(self, table, field, condition, *parameters, **kwparameters):
        """
        return all data with the conditions
        
        :return: list 
        """
        table = self.prefix + table
        query = "SELECT " + field + " FROM " + table + " " + condition
        return self.query(query, *parameters, **kwparameters)

    def get(self, table, field, condition, *parameters, **kwparameters):
        """
        return the latest line of data with the conditions
        
        :return: dict
        """
        data = self.select(table, field, condition, *parameters, **kwparameters)
        return data[0] if data else []

    def count(self, table, field, condition, *parameters, **kwparameters):
        """
        The number of data with the conditions

        :return: int
        """
        table = self.prefix + table
        field = 'COUNT(' + field + ') AS rows_count'
        query = "SELECT " + field + " FROM " + table + " " + condition
        rows_count = self.query(query, *parameters, **kwparameters)
        if rows_count:
            return int(rows_count[0]["rows_count"])
        else:
            return 0
