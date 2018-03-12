Welcome to saiorm /saɪɔ:m/,塞翁
===============================

Saiorm is a simple library for accessing database.
It will take you have a very easy way to use SQL database.

.. We want it to be an asynchronous framework,but not now.

**Method:**

- Method **insert, select, update, delete, execute, executemany, increase, decrease** should be executed **finally**,they will take effect immediately.

- Method **last_sql** return the latest executed sql.

- Method **get_fields_name** get a list of all fields name, cache them by default.

- Method **where** could be dict or str type. **IN** require a string or a sequence with str type.

- Method **select** and **get** return data only.

- Method **update**, **delete**, **execute** return a dict,including lastrowid, rowcount, rownumber, sql.

- Various method join,should use string param for method join and method where.

**ATTENTION**

1. Saiorm does not convert value type in condition(limit,order_by,group_by,
various join etc.),method where not convert value type in native functions and IN.
If you want to use the values passed from user,you must check them,
because it's easily to triggering injection vulnerability.

2. Saiorm require python3 and pymysql.

3. Support MySQL only,you can inherit from saiorm.base.BaseDB to support other types
of database with the same API,like siaorm.MySQL.ChainDB.

4. You can add "`" as a prefix to set the field to native function in method select and update.

Initialization
~~~~~~~~~~~~~~

.. code:: python

    import saiorm

    DB = saiorm.init()  # with no table name prefix
    # DB = saiorm.init(table_name_prefix="abc_") # with table name prefix
    DB.connect({"host": "", "port": 3306, "database": "", "user": "", "password": ""})
    table = DB.table("xxx")

Usage for calling mysql function only
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    DB.select("`NOW()")
    DB.select("`SUM(1+2)")

will transform to SQL

.. code:: sql

    SELECT NOW();
    SELECT SUM(1+2);

Usage for select and get
~~~~~~~~~~~~~~~~~~~~~~~~

1. select will return all data

2. get will modify _limit attribute automatically,then return the latest line only.
**If you call get method, limit method will be overwrited**

3. select and get receive a fields param.

.. code:: python

    # select all fields
    table.select()

    # get the latest line
    table.order_by("id DESC").get()

    # kinds of params in where
        table.where({
        "a": 1,
        "b": ("BETWEEN", "1", "2"),
        "c": ("`ABS(?)", "2"),
        "d": ("!=", 0),
        "e": ("IN", "1,2,3"),
        "f": "`NOW()",
    }).select("e,f")

will transform to SQL

.. code:: sql

    SELECT * FROM xxx ;
    SELECT * FROM xxx  ORDER BY id DESC LIMIT 1;
    SELECT e,f FROM xxx WHERE a=1 AND b BETWEEN '1' AND '2' AND c=ABS(2) AND d!=0 AND e IN (1,2,3) AND f=NOW() ;

Usage for update
~~~~~~~~~~~~~~~~

If you want use native function,you can pass a tuple.

.. code:: python

    table.where({
        "a": ("IN", ["1", "2", "3"]),
        "b": ("`ABS(?)", "2"),
    }).update({
        "c": "`ABS(2)",
        "d": ("`ABS(?)", 3),
        "e": "2",
    })

will transform to SQL

.. code:: sql

    UPDATE xxx SET c=ABS(2),d=ABS(3),e='2' WHERE a IN (1,2,3) AND b=ABS(2) ;


Usage for insert
~~~~~~~~~~~~~~~~

insert function support two kinds of data

.. code:: python

    # use natural dict
    table.insert({
        "a": "1",
        "b": "2",
    })

    # use split dict
    table.insert({
        "fields": ["a", "b"],
        "values": ["1", "2"],
    })

    # use natural dict in list, SQL will in one line
    table.insert_many([{
        "a": "1",
        "b": "2",
    }, {
        "a": "3",
        "b": "4",
    }, {
        "a": "5",
        "b": "6",
    }])

    # use split dict in list, SQL will in one line
    table.insert_many({
        "fields": ["a", "b"],
        "values": [
            ["1", "2"],
            ["3", "4"],
            ["5", "6"]
        ]
    })


will transform to SQL

.. code:: sql

    INSERT INTO xxx (a,b) VALUES ('1','2');
    INSERT INTO xxx (a,b) VALUES ('1','2');
    INSERT INTO xxx (a,b) VALUES ('1','2'),('3','4'),('5','6');
    INSERT INTO xxx (a,b) VALUES ('1','2'),('3','4'),('5','6');

If use split dict,key fields is not necessary,it will insert by the order of table struct.

Usage for delete
~~~~~~~~~~~~~~~~

By default, **delete** must have **where** condition,or you can pass strict=False when initialization.

.. code:: python

    table.where({
        "a": "1",
        "b": "2",
        "c": ("`ABS(?)", "2"),
    }).delete()

    table.delete()  # will not be executed, or set strict=False when initialization

will transform to SQL

.. code:: sql

    DELETE FROM xxx WHERE a='1' AND b='2' AND c=ABS(2) ;
    DELETE FROM xxx ;

Usage for increase
~~~~~~~~~~~~~~~~

Numerical field increase

.. code:: python

    table.increase("a", 1)

will transform to SQL

.. code:: sql

    UPDATE xxx SET a=a+1

Usage for decrease
~~~~~~~~~~~~~~~~

Numerical field decrease

.. code:: python

    table.decrease("a", 1)

will transform to SQL

.. code:: sql

    UPDATE xxx SET a=a-1

where condition
~~~~~~~~~~~~~~~

.. code:: python

    table.where({
        "a": 1,
        "b": ("BETWEEN", "1", "2"),
        "c": ("ABS(?)", "2"),
        "d": ("!=", 0),
        "e": ("IN", "1,2,3"),
        "f": "NOW()",
    }).select("e,f")

- must check param to prevent injection vulnerabilities.

- when calling native mysql function the param placeholder could be ?.

- condition will be equals to value,or pass a tuple or list, and set the first item to change it.

- use IN or BETWEEN should pass a tuple or list.

- pass string type is allowed,you should join param into this string.

Method Shorthand
~~~~~~~~~~~~~~~~

| t equals to table
| w equals to where
| ob equals to order_by
| l equals to limit
| gb equals to group_by
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