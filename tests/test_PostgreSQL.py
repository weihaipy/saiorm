import decimal
import os
import sys
import unittest

sys.path.append(os.path.abspath('..'))

import saiorm
import tests.database_conf as database_conf
import tests.util as util

conf = database_conf.PosgresSQL

test_sql_file_path = os.path.join(os.getcwd(), conf["test_sql_file_name"])

DB = saiorm.init(driver="PostgreSQL", table_name_prefix=conf["table_name_prefix"])
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

    # def test_get(self):
    #     res = DB.get("`SUM(1+2) as s")
    #     self.assertEqual(decimal.Decimal("3"), res["s"])
    #
    # def test_insert_natural_dict(self):
    #     table = DB.table("user")
    #     field = "name"
    #     name = "test_insert_natural_dict"
    #
    #     table.insert({
    #         field: name,
    #         "phone": "13512345678"
    #     })
    #
    #     res = table.where([(field, name)]).get(field)
    #     self.assertEqual(name, res[field])
    #
    # def test_insert_split_dict(self):
    #     table = DB.table("user")
    #     field = "name"
    #     name = "test_insert_split_dict"
    #
    #     table.insert({
    #         "fields": [field, "phone"],
    #         "values": [name, "13612345678"],
    #     })
    #
    #     res = table.where([(field, name)]).get(field)
    #     self.assertEqual(name, res[field])
    #
    # def test_insert_many(self):
    #     table = DB.table("user")
    #     field = "name"
    #     name = "test_insert_many"
    #
    #     table.insert_many([{
    #         field: name + "_1",
    #         "phone": "14012345678",
    #     }, {
    #         field: name + "_2",
    #         "phone": "14112345678",
    #     }, {
    #         field: name + "_3",
    #         "phone": "14212345678",
    #     }])
    #
    #     res = table.where([(field, name + "_1")]).get(field)
    #     self.assertEqual(name + "_1", res[field])
    #
    # def test_delete(self):
    #     table = DB.table("user")
    #     field = "name"
    #     table.where([("id", 1)]).delete()
    #     res_1 = table.where([("id", 1)]).get(field)
    #     res_2 = table.where([("id", 2)]).get(field)
    #     self.assertEqual(1, len(res_1) + len(res_2))
    #
    # def test_update_and_where(self):
    #     table = DB.table("blog")
    #     field = "content"
    #     content = "test_update"
    #
    #     table.where([
    #         ("id", 4),
    #     ]).update({
    #         field: content
    #     })
    #
    #     res = table.where([("id", 4)]).get(field)
    #     self.assertEqual(content, res[field])
    #
    # def test_increase(self):
    #     table = DB.table("user")
    #     field = "login_times"
    #     table.where([
    #         ("id", 4),
    #     ]).increase(field, 1)
    #
    #     res = table.where([("id", 4)]).get(field)
    #     self.assertEqual(445, res[field])
    #
    # def test_decrease(self):
    #     table = DB.table("user")
    #     field = "login_times"
    #     table.where([
    #         ("id", 3),
    #     ]).decrease(field, 1)
    #
    #     res = table.where([("id", 3)]).get(field)
    #     self.assertEqual(332, res[field])
    #
    # def test_order_by(self):
    #     table = DB.table("login_log")
    #     res = table.where([
    #         ("user_id", 1)
    #     ]).order_by("id desc").select("*")
    #     self.assertEqual(2, len(res))
    #
    # def test_group_by(self):
    #     table = DB.table("login_log")
    #     res = table.group_by("user_id").select("*")
    #     self.assertEqual(4, len(res))
    #
    # def test_limit(self):
    #     table = DB.table("login_log")
    #     res = table.limit(3).select("*")
    #     self.assertEqual(3, len(res))
    #
    # def test_inner_join(self):
    #     res = DB.table("user AS u").inner_join("login_log AS l").on("l.user_id = u.id").where([
    #         ("u.id", ">", 1),
    #         ("u.id", "<", 4)
    #     ]).select("u.*,l.*")
    #     self.assertEqual(3, len(res))
    #
    # def test_left_join(self):
    #     res = DB.table("user AS u").left_join("login_log AS l").on("l.user_id = u.id").where([
    #         ("u.id", ">", 1),
    #         ("u.id", "<", 4)
    #     ]).select("u.*,l.*")
    #     self.assertEqual(3, len(res))
    #
    # def test_right_join(self):
    #     res = DB.table("user AS u").right_join("login_log AS l").on("l.user_id = u.id").where([
    #         ("u.id", ">", 1),
    #         ("u.id", "<", 4)
    #     ]).select("u.*,l.*")
    #     self.assertEqual(3, len(res))
    #
    # def test_transaction(self):
    #     table = DB.table("user")
    #     field = "name"
    #     name = "test_transaction"
    #
    #     DB.begin()
    #
    #     table.insert({
    #         field: name,
    #         "phone": "19912345678"
    #     })
    #
    #     res_before_commit = table.where([(field, name)]).get("*")
    #     print("res_before_commit>>", res_before_commit)
    #     # todo 还是会自动提交
    #     # self.assertEqual(name, res_before_commit[field])
    #     # DB.commit()
    #     # res_after_commit = table.where([(field, name)]).get(field)
    #     # print("res_after_commit>>", res_after_commit)
    #     # self.assertEqual(3, len(res))


if __name__ == '__main__':
    unittest.main(verbosity=2)
