saiorm
======

`Saiorm <https://weihaipy.github.io/saiorm>`_  is a lightweight library for accessing database.It will take you have a easy way to use kinds of database with the same syntax,including SQL and NoSQL.

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
