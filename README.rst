saiorm
======

`Saiorm <https://weihaipy.github.io/saiorm>`_  is a very lightweight translator for accessing kinds of database with the same syntax,including SQL and NoSQL.

It only translate python code and arguments to database statement,no longer need models.Directly operate the data in the database. No data type conversion, minimize the performance loss.

If you want to support other database,just implement like siaorm.PostgreSQL.ChainDB.

like Saiorm.PostgreSQL.CoherentDB and add a few driver code to Saiorm.init.

Documentation
=============

 `Learn more <http://saiorm.readthedocs.io>`_.

Task
====

- [x] Support MySQL, MariaDB
- [x] Support PostgreSQL
- [x] Support SQL Server
- [x] Support SQLite
- [x] Support MongoDB

Note that MongoDB support **select,get,update,insert,insert_many,delete,increase,decrease,where,limit,order_by**

TODO
====

- NOT TEST:
    - FIX BUG IN USEING OR STATEMENT,CHANGE WHERE PARAM TO LIST INSTEAD OF DICT

    - CHANGE LINIT,USE LIMIT OFFSET

- SQL database:

    - Transaction::

        BEGIN
        COMMIT
        ROLLBACK

    - having

    - join: support FULL OUTER JOIN and FULL JOIN.

    - check auto commit in SQLite

- MongoDB::

    group
    native function,BETWEEN,IN etc.

- pyodbc::

    https://www.jb51.net/article/143212.htm

- 添加别名::

    select > fetchall
    get > fetchone

- 字符串的处理全部改用 f 字符串,提高速度

- 已知问题::

    暂无