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
    # def setUp(self):
    #     reset_database()
    #
    # def tearDown(self):
    #     pass

    def test_select(self):
        res = DB.select("`SUM(1+2) as s")[0]
        self.assertEqual(decimal.Decimal("3"), res["s"])

    def test_get(self):
        res = DB.get("`SUM(1+2) as s")
        self.assertEqual(decimal.Decimal("3"), res["s"])

    def test_insert(self):
        pass

    def test_insert_many(self):
        pass

    def test_delete(self):
        pass

    def test_update_and_where(self):
        field = "content"
        content = "test_update"

        DB.table("blog").where([
            ("id", 4),
        ]).update({
            field: content
        })

        res = DB.table("blog").where([("id", 4)]).get(field)
        self.assertEqual(content, res[field])

    def test_increase(self):
        field = "login_times"
        DB.table("user").where([
            ("id", 4),
        ]).increase(field, 1)

        res = DB.table("user").where([("id", 4)]).get(field)
        self.assertEqual(445, res[field])

    def test_decrease(self):
        pass

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
