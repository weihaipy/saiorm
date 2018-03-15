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
        logging.error("Error query:", query + ": " + str(parameters))
        logging.error("Error Exception:" + str(exception))

    def select(self, parameters):
        """
        TODO if want to limit fields,must set all of the fields to hide equals to 0 explicitly
        """
        condition = self.condition["where"]
        sort = self.condition.get("sort", "")
        skip = self.condition.get("skip", "")
        limit = self.condition.get("limit", "")

        try:
            eval_str = """getattr(self._db, self.condition["table"]).find(condition)"""
            if sort:
                eval_str += ".sort(" + str(sort) + ")"
            if skip:
                eval_str += ".skip(" + str(skip) + ")"
            if sort:
                eval_str += ".limit(" + str(limit) + ")"

            res = eval(eval_str)

            if self._return_query:  # generate query
                query_str_tmpl = "MongoDB {}.find({})"
                query_value = [self.condition["table"], str(condition)]
                if sort:
                    query_str_tmpl += ".sort({})"
                    query_value.append(str(sort))

                if skip:
                    query_str_tmpl += ".skip({})"
                    query_value.append(str(skip))

                if sort:
                    query_str_tmpl += ".limit({})"
                    query_value.append(str(limit))

                query = query_str_tmpl.format(*query_value)
            else:
                query = ""
            self.condition = {}  # reset condition
            return {
                "data": res or [],
                "query": query
            }
        except Exception as e:
            self._log_exception(e, "select", self.condition)
            raise

    def insert(self, parameters):
        try:
            getattr(self._db, self.condition["table"]).insert_one(parameters)
            return {
                "lastrowid": 0,  # the primary key id affected
                "rowcount": 0,  # number of rows affected
                "rownumber": 0,  # line number
                "query": "{}.insert_one({})".format(self.condition["table"],
                                                    str(parameters)) if self._return_query else ""  # query executed
            }
        except Exception as e:
            self._log_exception(e, "insert", self.condition)
            raise

    def insert_many(self, parameters):
        try:
            getattr(self._db, self.condition["table"]).insert_many(parameters)
            return {
                "lastrowid": 0,  # the primary key id affected
                "rowcount": 0,  # number of rows affected
                "rownumber": 0,  # line number
                "query": "{}.insert_many({})".format(self.condition["table"],
                                                     str(parameters)) if self._return_query else ""  # query executed
            }
        except Exception as e:
            self._log_exception(e, "insert_many", self.condition)
            raise

    def update(self, parameters):
        where = self.condition["where"]

        try:
            res = getattr(self._db, self.condition["table"]).update(where, parameters)
            # returns  {'n': 1, 'nModified': 1, 'ok': 1.0, 'updatedExisting': True}
            query = "{}.update({}, {})".format(self.condition["table"], str(where),
                                               str(parameters)) if self._return_query else ""
            self.condition = {}  # reset condition
            return {
                "lastrowid": 0,  # the primary key id affected
                "rowcount": res.get("nModified", res["n"]),  # number of rows affected
                "rownumber": 0,  # line number
                "query": query
                # query executed
            }
        except Exception as e:
            self._log_exception(e, "update", self.condition)
            raise

    def delete(self):
        where = self.condition["where"]
        try:
            res = getattr(self._db, self.condition["table"]).remove(where)
            # returns {'n': 2, 'ok': 1.0}
            query = "{}.remove({})".format(self.condition["table"],
                                           str(self.condition["where"])) if self._return_query else ""
            self.condition = {}  # reset condition
            return {
                "lastrowid": 0,  # the primary key id affected
                "rowcount": res.get("nRemoved", res["n"]),  # number of rows affected
                "rownumber": 0,  # line number
                "query": query  # query executed
            }
        except Exception as e:
            self._log_exception(e, "delete", self.condition)
            raise

    def group(self):
        # TODO use group()
        pass


class ChainDB(base.ChainDB):
    def connect(self, config_dict=None, return_query=False):
        if return_query:
            config_dict["return_query"] = return_query
        self.db = ConnectionMongoDB(**config_dict)

    def execute(self, *args, **kwargs):
        logging.warning("Saiorm does not support execute in MongoDB")
        return self

    def executemany(self, *args, **kwargs):
        logging.warning("Saiorm does not support executemany in MongoDB")
        return self

    def query(self, *args, **kwargs):
        logging.warning("Saiorm does not support query in MongoDB")
        return self

    def group_by(self, condition):
        logging.warning("Saiorm does not support group_by in MongoDB")
        return self

    def join(self, condition):
        logging.warning("Saiorm does not support join in MongoDB")
        return self

    def inner_join(self, condition):
        logging.warning("Saiorm does not support inner_join in MongoDB")
        return self

    def left_join(self, condition):
        logging.warning("Saiorm does not support left_join in MongoDB")
        return self

    def right_join(self, condition):
        logging.warning("Saiorm does not support right_join in MongoDB")
        return self

    def select(self, fields="*"):
        self.set_condition()
        res = self.db.select(fields)
        self.last_query = res["query"]
        return res["data"]

    def get(self, fields="*"):
        self._limit = 1
        self.set_condition()
        res = self.db.select(fields)
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
        logging.warning("Saiorm does not support get_fields_name in MongoDB")

    def set_condition(self):
        """
        set condition to MongoDB

        """
        res = {
            "table": self._table,
            "where": {},
            "sort": [],  # pymongo needs list type
            "limit": "0",
            "skip": "0"
        }
        sql = ""
        sql_values = []
        if self._where:
            # TODO support native function,BETWEEN,IN etc.
            if isinstance(self._where, dict):
                for k in self._where.keys():
                    if not k.startswith(">="):
                        res["where"][k] = {"$gte": self._where[k]}
                    elif not k.startswith(">"):
                        res["where"][k] = {"$gt": self._where[k]}
                    elif not k.startswith("<="):
                        res["where"][k] = {"$lte": self._where[k]}
                    elif not k.startswith("<"):
                        res["where"][k] = {"$lt": self._where[k]}
                    elif not k.startswith("!="):
                        res["where"][k] = {"$ne": self._where[k]}
                    elif not k.startswith("`"):
                        res["where"][k] = self._where[k]
            else:
                logging.error("Saiorm does not support str type where condition in MongoDB")

        if self._order_by:
            _order_by_list = self._order_by.split(",")
            for i in _order_by_list:
                i = i.lower().strip()
                if i.endswith(" desc"):
                    res["sort"].append((i[:-len(" desc")], -1))
                else:
                    res["sort"].append((i, 1))

        if self._limit:
            _limit = str(self._limit)
            if "," not in _limit:
                res["limit"] = _limit
            else:
                m, n = _limit.split(",")
                res["limit"] = n
                res["skip"] = m

        self.db.condition = res

        return sql, sql_values
