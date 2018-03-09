Welcome to saiorm /saɪɔ:m/,塞翁
===============================

Saiorm is a simple library for accessing database.
It will take you have a easy way to use SQl database.

**require**
python3, pymysql, torndb.

The goal is tb be an asynchronous framework,but not now.

**Saiorm support MySQL only now.**

Method **table** should be executed **at first**,it will reset all attributes.

Method **insert, select, update, delete** should be executed **finally**.

Select and get method will return a dict,include data, column_names, sql.
Other method and get will return a dict,include lastrowid, rowcount, rownumber, sql.

Initialization
~~~~~~~~~~~~~~

.. code:: python

    import saiorm

    DB = saiorm.CoherentDB()  # with no table name prefix
    # DB = saiorm.CoherentDB(table_name_prefix="abc_") # with table name prefix
    DB.connect({"host": "", "port": 3306, "database": "", "user": "", "password": ""})

Usage for select and get
~~~~~~~~~~~~~~~~~~~~~~~~

1. select will return all data
2. get will modify _limit attribute automatically,and return the latest line only.
**If you call get method, limit method will be overwrited**

select and get receive a fields param.

.. code:: python

    # select all fields
    DB.table("table").select()

    # get the latest line
    DB.table("table").order_by("id DESC").get()

    # call mysql function with param(param should be str)
    DB.table("table").where({
        "a": 1,
        "b": 2,
        "c": ("ABS({})", "2"),
        "d": "now()",
    }).select("zz,xx")

    # call mysql function with no param
    DB.select("now()")


will transform to SQL

.. code:: sql

    SELECT * FROM table ;
    SELECT * FROM table  ORDER BY id DESC LIMIT 1;
    SELECT zz,xx FROM table WHERE a=1 AND b=2 AND c=ABS(2) AND d=now() ;
    SELECT now();


Usage for update
~~~~~~~~~~~~~~~~

If you want use native function,you can pass a tuple.

.. code:: python

    DB.table("table").where({
        "a": 1,
        "b": 2,
        "c": ("ABS({})", "2"),
        "d": "now()",
    }).update({
        "x": "1",
        "y": "2",
    })


will transform to SQL

.. code:: sql

    UPDATE table SET x=%s,y=%s WHERE a=1 AND b=2 AND c=ABS(2) AND d=now() ;


Usage for insert
~~~~~~~~~~~~~~~~

insert function support two kinds of data

.. code:: python

    # use dict 1 natural
    DB.table("table").insert({
        "a": "1",
        "b": "2",
    })

    # use dict 2
    DB.table("table").insert({
        "fields": ["a", "b"],
        "values": ["1", "2"],

    })

    # use natural dict in list, SQL statement will in one line
    DB.table("table").insert_many([{
        "a": "1",
        "b": "2",
    }, {
        "a": "3",
        "b": "4",
    }])

    # use split dict in list, SQL statement will in one line
    DB.table("table").insert_many({
        "fields": ["a", "b"],
        "values": [
            ["1", "2"],
            ["3", "4"],
            ["5", "6"]
        ]
    })


will transform to SQL

.. code:: sql

    INSERT INTO table (a,b) VALUES (%s,%s);
    INSERT INTO table (a,b) VALUES (%s,%s);
    INSERT INTO table (a,b) VALUES (%s,%s);
    INSERT INTO table (a,b) VALUES (%s,%s,%s);


Usage for delete
~~~~~~~~~~~~~~~~

By default, delete must have where condition,or you can pass strict=False when initialization.

.. code:: python

    DB.table("table").where({
        "a": 1,
        "b": 2,
        "c": ("ABS({})", "2"),
        "d": "now()",
    }).delete()

    DB.table("table").delete()  # will not execute, or set strict=False when initialization

will transform to SQL

.. code:: sql

    DELETE FROM table WHERE a=1 AND b=2 AND c=ABS(2) AND d=now() ;
    DELETE FROM table ;

Usage for increase
~~~~~~~~~~~~~~~~

Numerical field increase

.. code:: sql

    DB.table("xxx").increase("a", 1)


Usage for decrease
~~~~~~~~~~~~~~~~

Numerical field decrease

.. code:: sql

    DB.table("xxx").decrease("a", 1)


Usage for get_fields_name
~~~~~~~~~~~~~~~~~~~~~~~~~

Get all fields name of the table and cache them(by default)

.. code:: sql

    DB.table("xxx").get_fields_name()


Other usage
~~~~~~~~~~~

Get the latest SQL

.. code:: python

    DB.last_sql

Method Shorthand
~~~~~~~~~~~~~~~~

| t equals to table
| w equals to where
| o equals to order_by
| l equals to limit
| g equals to group_by
| j equals to join
| ij equals to inner_join
| lj equals to left_join
| rj equals to right_join
| s equals to select
| i equals to insert
| im equals to insert_many
| u equals to update
| d equals to delete
| inc equals to increase
| dec equals to decrease