## Welcome to saiorm

Saiorm is a simple library for accessing database from the asyncio framework.

It will take you have a easy way to use SQl database. 

### Usage

Markdown is a lightweight and easy-to-use syntax for styling your writing. It includes conventions for

```python
DB("table_name").where({
    "a": 1,
    "b": 2,
    "c": ("ABS({})", "3"),  # call mysql function with param
    "d": "now()",  # call mysql function with no param
}).select("sss,ds,dsd,dfd")

DB().select("now()")
```

will transform to

```mysql
SELECT sss,ds,dsd,dfd FROM table_name WHERE a=1 AND b=2 AND c=ABS(3) AND d=now() ;
SELECT now();
```

For more details see [GitHub Flavored Markdown](https://guides.github.com/features/mastering-markdown/).

### Plan

I will support MySQL first,and then PostgreSQL etc.


