#!/usr/bin/env python
# -*- coding:utf-8 -*-

import logging

try:
    from . import utility
except ImportError:
    import utility

GraceDict = utility.GraceDict
is_array = utility.is_array


class BaseDB(object):
    """
    Implement database chain  operation.

    After initialization with table name,use config_db to set connected database.

    In JOIN,use ### as table name prefix placeholder.
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

    def _reset(self):
        """reset param when call again"""
        # self._table = "" # keep table name
        self._where = ""
        self._order_by = ""
        self._group_by = ""
        self._limit = ""
        self._inner_join = ""
        self._left_join = ""
        self._right_join = ""
        self._on = ""
        self.last_sql = ""  # latest executed sql

    def connect(self, config_dict=None):
        """
        set a connected torndb.Connection

        :param config_dict: dict,config to connect database
        """
        raise NotImplementedError("You must implement it in subclass")

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
        if self.table_name_prefix and "###" in condition:
            condition = condition.replace("###", self.table_name_prefix)

        self._inner_join = condition
        return self

    def inner_join(self, condition):
        if self.table_name_prefix and "###" in condition:
            condition = condition.replace("###", self.table_name_prefix)
        self._inner_join = condition
        return self

    def left_join(self, condition):
        if self.table_name_prefix and "###" in condition:
            condition = condition.replace("###", self.table_name_prefix)
        self._left_join = condition
        return self

    def right_join(self, condition):
        if self.table_name_prefix and "###" in condition:
            condition = condition.replace("###", self.table_name_prefix)
        self._right_join = condition
        return self

    def select(self, fields="*"):
        # raise NotImplementedError("You must implement it in subclass")

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
        self.last_sql = res["sql"]
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
        return self.select(fields)

    def update(self, dict_data=None):
        if not dict_data:
            return False
        fields, values = self.split_update_fields_value(dict_data)
        condition_sql, condition_values = self.parse_condition()
        sql = self.gen_update(fields, condition_sql)
        values += condition_values
        values = tuple(values)
        res = self.execute(sql, *values)
        self.last_sql = res["sql"]
        return res

    def gen_update(self, fields, condition):
        raise NotImplementedError("You must implement it in subclass")

    def split_update_fields_value(self, dict_data):
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

        values_sign = ",".join(["%s" for i in values])
        if fields:
            sql = self.gen_insert_with_fields(fields, values_sign)
        else:
            sql = self.gen_insert_without_fields(values_sign)
        values = tuple(values)
        res = self.execute(sql, *values)
        self.last_sql = res["sql"]
        return res

    def gen_insert_with_fields(self, fields, condition):
        raise NotImplementedError("You must implement it in subclass")

    def gen_insert_without_fields(self, fields):
        raise NotImplementedError("You must implement it in subclass")

    def insert_many(self, dict_data=None):
        """
        insert one line,,support rwo kinds data,such as insert,
        but the values should be wraped with list or tuple

        """
        if not dict_data:
            return False

        fields = ""  # 所有的字段

        # 列表或元祖的结构必须一样
        if is_array(dict_data):
            dict_data_item_1 = dict_data[0]  # 应该是字典
            keys = dict_data_item_1.keys()
            fields = ",".join(keys)
            values = [tuple(i.values()) for i in dict_data]  # 字典的 values 先转换
            values_sign = ",".join(["%s" for f in keys])
        elif isinstance(dict_data, dict):  # 字段名和值分开传
            keys = dict_data.get("fields")
            if keys:
                if "values" in keys and len(keys) == 1:  # split dict without fields
                    fields = None
                else:
                    fields = ",".join(dict_data["fields"])
            values = list([v for v in dict_data["values"]])  # 字典的 values 先转换
            values_sign = ",".join(["%s" for f in keys])
        else:
            logging.error("Param should be list or tuple or dict")
            return False

        if fields:
            sql = self.gen_insert_with_fields(fields, values_sign)
        else:
            sql = self.gen_insert_without_fields(values_sign)

        res = self.executemany(sql, values)
        self.last_sql = res["sql"]
        return res

    def gen_insert_many_with_fields(self, fields, condition):
        raise NotImplementedError("You must implement it in subclass")

    def gen_insert_many_without_fields(self, fields):
        raise NotImplementedError("You must implement it in subclass")

    def delete(self):
        if self.strict and not self._where:
            logging.warning("without where condition,can not delete")
            return False

        condition_sql, condition_values = self.parse_condition()
        sql = self.gen_delete(condition_sql)
        res = self.execute(sql, *condition_values)
        self.last_sql = res["sql"]
        return res

    def gen_delete(self, condition):
        raise NotImplementedError("You must implement it in subclass")

    def increase(self, field, step=1):
        """number field Increase """
        sql = self.gen_increase(field, str(step))
        res = self.execute(sql)
        self.last_sql = res["sql"]
        return res

    def gen_increase(self, field, step):
        raise NotImplementedError("You must implement it in subclass")

    def decrease(self, field, step=1):
        """number field decrease """
        sql = self.gen_decrease(field, str(step))
        res = self.execute(sql)
        self.last_sql = res["sql"]
        return res

    def gen_decrease(self, field, step):
        raise NotImplementedError("You must implement it in subclass")

    def get_fields_name(self):
        """return all fields of table"""
        if not self._table:
            return []

        if self.cache_fields_name and self._cached_fields_name.get(self._table):
            return self._cached_fields_name.get(self._table)
        else:
            res = self.db.query_return_detail(self.gen_get_fields_name())
            fields_name = res["column_names"]
            self._cached_fields_name[self._table] = fields_name

            return fields_name

    def gen_get_fields_name(self):
        """get one line from table"""
        raise NotImplementedError("You must implement it in subclass")

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

    def parse_condition(self):
        raise NotImplementedError("You must implement it in subclass")
