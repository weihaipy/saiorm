import decimal
import os
import sys
import unittest

sys.path.append(os.path.abspath('..'))

import saiorm
import tests.database_conf as database_conf
import tests.util as util

conf = database_conf.MySQL

test_sql_file_path = os.path.join(os.getcwd(), conf["test_sql_file_name"])

DB = saiorm.init(driver="MySQL", table_name_prefix=conf["table_name_prefix"])  # mysql with table name prefix
DB.connect({
    "host": conf["host"],
    "port": conf["port"],
    "database": conf["database"],
    "user": conf["user"],
    "password": conf["password"]
})

has_reset_database = False


def reset_database():
    """reset a new test database"""
    global has_reset_database
    if not has_reset_database:
        statements = util.split_sql_file_into_statements_list(test_sql_file_path)
        for s in statements:
            DB.execute(s)

        has_reset_database = True


class TestFunctions(unittest.TestCase):
    def setUp(self):
        reset_database()

    #
    # def tearDown(self):
    #     pass

    def test_select(self):
        res = DB.select("`SUM(1+2) as s")[0]
        self.assertEqual(decimal.Decimal("3"), res["s"])

    def test_get(self):
        res = DB.get("`SUM(1+2) as s")
        self.assertEqual(decimal.Decimal("3"), res["s"])

    def test_insert_natural_dict(self):
        field = "name"
        name = "test_insert_natural_dict"
        table = DB.table("user")
        table.insert({
            field: name,
            "phone": "13512345678"
        })

        res = table.where([("name", name)]).get(field)
        self.assertEqual(name, res[field])

    def test_insert_split_dict(self):
        field = "name"
        name = "test_insert_split_dict"
        table = DB.table("user")
        table.insert({
            "fields": [field, "phone"],
            "values": [name, "13612345678"],
        })

        res = table.where([("name", name)]).get(field)
        self.assertEqual(name, res[field])

    def test_insert_many(self):
        field = "name"
        name = "test_insert_many"
        table = DB.table("user")
        table.insert_many([{
            field: name + "_1",
            "phone": "14012345678",
        }, {
            field: name + "_2",
            "phone": "14112345678",
        }, {
            field: name + "_3",
            "phone": "14212345678",
        }])

        res = table.where([("name", name + "_1")]).get(field)
        self.assertEqual(name + "_1", res[field])

    def test_delete(self):
        table = DB.table("user")
        field = "name"
        table.where([("id", 1)]).delete()
        res = table.where([("id", 2)]).get(field)
        self.assertEqual(1, len(res))

    def test_update_and_where(self):
        table = DB.table("blog")
        field = "content"
        content = "test_update"

        table.where([
            ("id", 4),
        ]).update({
            field: content
        })

        res = table.where([("id", 4)]).get(field)
        self.assertEqual(content, res[field])

    def test_increase(self):
        table = DB.table("user")
        field = "login_times"
        table.where([
            ("id", 4),
        ]).increase(field, 1)

        res = table.where([("id", 4)]).get(field)
        self.assertEqual(445, res[field])

    def test_decrease(self):
        table = DB.table("user")
        field = "login_times"
        table.where([
            ("id", 3),
        ]).decrease(field, 1)

        res = table.where([("id", 3)]).get(field)
        self.assertEqual(332, res[field])

    def test_query(self):
        pass

    def test_execute(self):
        pass

    def test_executemany(self):
        pass

    def test_where(self):
        pass

    def test_order_by(self):
        pass

    def test_group_by(self):
        pass

    def test_limit(self):
        pass

    def test_inner_join(self):
        pass

    def test_left_join(self):
        pass

    def test_right_join(self):
        pass


if __name__ == '__main__':
    unittest.main(verbosity=2)
