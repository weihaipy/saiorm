saiorm
======

`Saiorm <https://weihaipy.github.io/saiorm>`_  is a simple library for accessing database.
It will take you have a easy way to use SQL database.

.. The goal is to be an asynchronous framework,but not now.

Documentation
-------------

 `Learn more <http://saiorm.readthedocs.io>`_.

Task
----

- [x] transform to SQL
- [x] Support MySQL
- [x] Support PostgreSQL
- [ ] Support SQLserver now
- [ ] Support Oracle Database

***Trouble in SQL Server***

- not support LIMIT,need modify method select and method where
- not return the latest query

If you want to support other database,just implement with the same API

like Saiorm.PostgreSQL.CoherentDB and add a few driver code to Saiorm.init.
