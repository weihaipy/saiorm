## Welcome to saiorm

Saiorm is a simple library for accessing database from the asyncio framework.

It will take you have a easy way to use SQl database. 

### Usage
1. only param
2. call mysql function with param(param should be str)
3. call mysql function with no param

```python
DB("table").where({
    "a": "1",
    "b": ("ABS({})", "3"),  
    "c": "now()",  
}).select("zz,xx")

DB().select("now()")
```

will transform to

```sql
SELECT zz,xx FROM table WHERE a=1 AND b=ABS(3) AND c=now() ;
SELECT now();
```

For more details see [GitHub Flavored Markdown](https://guides.github.com/features/mastering-markdown/).

### Plan

I will support MySQL first,and then PostgreSQL etc.


