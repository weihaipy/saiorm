#!/usr/bin/env python
# -*- coding:utf-8 -*-


def init(driver="MySQL", **kwargs):
    if driver.lower() == "mysql":
        from .MySQL import ChainDB
        return ChainDB(**kwargs)
    elif driver.lower() == "postgresql":
        from .PostgreSQL import ChainDB
        return ChainDB(**kwargs)
    elif driver.lower() == "sqlserver":
        from .SQLServer import ChainDB

        print("You need install pymssql first,and this type has not been tested")
        return ChainDB(**kwargs)
    else:
        raise ValueError("Init saiorm with wrong database driver type")
