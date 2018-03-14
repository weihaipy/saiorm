#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Support MongoDB

bases on torndb
"""
import logging
import time

import pymongo

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


class ConnectionMongoDB(object):
    def __init__(self, host, port, database, user=None, password=None,
                 max_idle_time=7 * 3600, return_query=False):
        self.host = host
        self.database = database
        self.max_idle_time = float(max_idle_time)
        self._return_query = return_query
        self.condition = {}  # like WHERE, ORDER BY, LIMIT etc. in SQL
        self.client = None  # Mongo client

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
            self.client.close()
            self._db = None

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.close()
        database = self._db_args["database"]
        user = self._db_args["user"]
        password = self._db_args["password"]
        client = pymongo.MongoClient(host=self._db_args["host"], port=self._db_args["port"])
        if user and password:
            client.authenticate(user, password)
        self._db = getattr(client, database)
        self.client = client

    def _log_exception(self, exception, query, parameters):
        """log exception when execute query"""
        logging.error("Error on MongoDB:" + self.host)
        logging.error("Error query:", query + str(parameters))
        logging.error("Error Exception:" + str(exception))

    # TODO sort limit offset
    def select(self, parameters):
        condition = self.condition["where"]
        try:
            res = getattr(self._db, self.condition["table"]).find(condition)
            query = "MongoDB {}.find({})".format(self.condition["table"], str(condition)) if self._return_query else ""
            self.condition = {}
            return {
                "data": res or [],
                "query": query
            }
        except Exception as e:
            self._log_exception(e, "select", self.condition)
            raise

    def get(self, parameters):
        condition = self.condition["where"]
        try:
            res = getattr(self._db, self.condition["table"]).find_one(condition)
            query = "MongoDB {}.find_one:".format(self.condition["table"], str(condition)) if self._return_query else ""
            self.condition = {}
            return {
                "data": res or [],
                "query": query
            }
        except Exception as e:
            self._log_exception(e, "get", self.condition)
            raise

    def insert(self, parameters):
        try:
            getattr(self._db, self.condition["table"]).insert_one(parameters)
            query = "MongoDB {}.insert_one({})".format(self.condition["table"],
                                                       str(parameters)) if self._return_query else ""
            return {
                "lastrowid": 0,  # 影响的主键id
                "rowcount": 0,  # 影响的行数
                "rownumber": 0,  # 行号
                "query": query  # 执行的语句
            }
        except Exception as e:
            self._log_exception(e, "insert", self.condition)
            raise

    def insert_many(self, parameters):
        try:
            getattr(self._db, self.condition["table"]).insert_many(parameters)
            query = "MongoDB {}.insert_many({})".format(self.condition["table"],
                                                       str(parameters)) if self._return_query else ""
            return {
                "lastrowid": 0,  # 影响的主键id
                "rowcount": 0,  # 影响的行数
                "rownumber": 0,  # 行号
                "query": query  # 执行的语句
            }
        except Exception as e:
            self._log_exception(e, "insert_many", self.condition)
            raise

    def update(self, parameters):
        condition = self.condition["where"]

        try:
            res = getattr(self._db, self.condition["table"]).update(condition, parameters)
            # returns  {'n': 1, 'nModified': 1, 'ok': 1.0, 'updatedExisting': True}
            query = "MongoDB {}.update({}, {})".format(self.condition["table"], str(self.condition["where"]),
                                                       str(parameters)) if self._return_query else ""
            self.condition = {}
            return {
                "lastrowid": 0,  # 影响的主键id
                "rowcount": res["n"],  # 影响的行数
                "rownumber": 0,  # 行号
                "query": query  # 执行的语句
            }
        except Exception as e:
            self._log_exception(e, "update", self.condition)
            raise

    def delete(self):
        condition = self.condition["where"]
        try:
            res = getattr(self._db, self.condition["table"]).remove(condition)
            query = "MongoDB {}.remove({})".format(self.condition["table"],
                                                   str(self.condition["where"])) if self._return_query else ""
            self.condition = {}
            # returns {'n': 2, 'ok': 1.0}
            return {
                "lastrowid": 0,  # 影响的主键id
                "rowcount": res["n"],  # 影响的行数
                "rownumber": 0,  # 行号
                "query": query  # 执行的语句
            }
        except Exception as e:
            self._log_exception(e, "delete", self.condition)
            raise


class ChainDB(base.ChainDB):
    def connect(self, config_dict=None, return_query=False):
        if return_query:
            config_dict["return_query"] = return_query
        self.db = ConnectionMongoDB(**config_dict)

    def execute(self, *args, **kwargs):
        logging.warning("Saiorm do not support execute in MongoDB")
        return self

    def executemany(self, *args, **kwargs):
        logging.warning("Saiorm do not support executemany in MongoDB")
        return self

    def query(self, *args, **kwargs):
        logging.warning("Saiorm do not support query in MongoDB")
        return self

    def group_by(self, condition):
        logging.warning("Saiorm do not support group_by in MongoDB")
        return self

    def join(self, condition):
        logging.warning("Saiorm do not support join in MongoDB")
        return self

    def inner_join(self, condition):
        logging.warning("Saiorm do not support inner_join in MongoDB")
        return self

    def left_join(self, condition):
        logging.warning("Saiorm do not support left_join in MongoDB")
        return self

    def right_join(self, condition):
        logging.warning("Saiorm do not support right_join in MongoDB")
        return self

    def select(self, fields="*"):
        self.set_condition()
        res = self.db.select(fields)
        self.last_query = res["query"]
        return res["data"]

    def get(self, fields="*"):
        self.set_condition()
        res = self.db.get(fields)
        self.last_query = res["query"]
        return res["data"]

    def update(self, dict_data=None):
        self.set_condition()
        res = self.db.update(dict_data)
        self.last_query = res["query"]
        return res

    def insert(self, dict_data=None):
        self.set_condition()
        if "fields" in dict_data and "values" in dict_data:  # split dict
            dict_data = {k: dict_data["values"][index]
                         for index, k in enumerate(dict_data["fields"])}

        res = self.db.insert(dict_data)
        self.last_query = res["query"]
        return res

    def insert_many(self, dict_data=None):
        self.set_condition()
        if isinstance(dict_data, dict):
            keys = dict_data.keys()
            if "fields" in keys and "values" in keys:
                dict_data = [{k: v[index]}
                             for index, k in enumerate(dict_data["fields"])
                             for v in dict_data["values"]]

        res = self.db.insert_many(dict_data)
        self.last_query = res["query"]
        return res

    def delete(self):
        self.set_condition()
        res = self.db.delete()
        self.last_query = res["query"]
        return res

    def increase(self, field, step=1):
        """number field Increase """
        self.set_condition()
        res = self.db.update({"$inc": {field: step}})
        self.last_query = res["query"]
        return res

    def decrease(self, field, step=1):
        """number field decrease """
        self.set_condition()
        res = self.db.update({"$inc": {field: 0 - step}})
        self.last_query = res["query"]
        return res

    def get_fields_name(self):
        logging.warning("Saiorm do not support get_fields_name in MongoDB")

    def set_condition(self):
        """
        set condition to MongoDB

        """
        res = {
            "table": self._table,
            "where": {},
            "sort": {},
            "limit": 0,
            "offset": 0
        }
        sql = ""
        sql_values = []
        if self._where:
            if isinstance(self._where, dict):
                for k in self._where.keys():
                    if not k.startswith("`"):
                        res["where"][k] = self._where[k]
            else:
                logging.error("Saiorm do not support str type where condition in MongoDB")

        if self._order_by:
            _order_by = self._order_by.lower().strip()
            if _order_by.endswith(" desc"):
                res["sort"] = {self._order_by[:-len(" desc")]: -1}
            else:
                res["sort"] = {self._order_by: 1}

        if self._limit:
            _limit = str(self._limit)
            if "," not in _limit:
                res["limit"] = self._limit
            else:
                m, n = _limit.split(",")
                res["limit"] = n
                res["offset"] = m

        self.db.condition = res

        return sql, sql_values
