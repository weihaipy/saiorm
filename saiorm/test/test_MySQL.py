import unittest

import saiorm
from . import database_conf

conf = database_conf.MySQL

DB = saiorm.init(driver="MySQL", table_name_prefix=conf["table_name_prefix"])  # mysql with table name prefix
DB.connect({
    "host": conf["host"],
    "port": conf["port"],
    "database": conf["database"],
    "user": conf["user"],
    "password": conf["password"]
})


class TestFunctions(unittest.TestCase):
    def setUp(self):
        print("Test start")
        # todo 检查测试数据库是否存在,充值一个新的测试数据库

    def tearDown(self):
        print('Test stop')

    def test_insert(self):
        # self.assertEqual(3, add(1, 2))
        # self.assertNotEqual(3, add(2, 2))
        pass

    def test_insert_many(self):
        pass

    def test_delete(self):
        pass

    def test_update(self):
        pass

    def test_increase(self):
        pass

    def test_decrease(self):
        pass

    def test_select(self):
        pass

    def test_get(self):
        pass

    def test_query(self):
        pass

    def test_execute(self):
        pass

    def test_executemany(self):
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
