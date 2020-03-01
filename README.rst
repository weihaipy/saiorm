saiorm
======

`Saiorm <https://weihaipy.github.io/saiorm>`_  is a very lightweight translator for accessing kinds of database with the same syntax,including SQL and NoSQL.

It only translate python code and arguments to database statement,no longer need models.Directly operate the data in the database. No data type conversion, minimize the performance loss.

If you want to support other database,just implement like siaorm.PostgreSQL.ChainDB.

like Saiorm.PostgreSQL.CoherentDB and add a few driver code to Saiorm.init.

**python version** python3.6 and later.

Documentation
=============

 `Learn more <http://saiorm.readthedocs.io>`_.

MongoDB only support **select,get,update,insert,insert_many,delete,increase,decrease,where,limit,order_by**

TODO
====

- NOT TEST:
    - FIX BUG IN USEING OR STATEMENT,CHANGE WHERE PARAM TO LIST INSTEAD OF DICT

    - CHANGE LINIT,USE LIMIT OFFSET

- SQL database:

    - having

    - check auto commit in SQLite

- MongoDB::

    group
    native function,BETWEEN,IN etc.

- pyodbc::

    https://www.jb51.net/article/143212.htm

- 字符串的处理全部改用 f 字符串,提高速度

- 已知问题::

    autocommit 在 MySQL 事务中无效

    MySQL 报错 sql_mode=only_full_group_by 的解决方法:
    select @@global.sql_mode;
    set @@global.sql_mode='';  去掉前面查询的 ONLY_FULL_GROUP_BY


