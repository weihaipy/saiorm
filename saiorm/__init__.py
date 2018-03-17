#!/usr/bin/env python
# -*- coding:utf-8 -*-
from .utility import GraceDict


def init(driver="MySQL", **kwargs):
    if driver.lower() == "mysql":
        from .MySQL import ChainDB
        return ChainDB(**kwargs)
    if driver.lower() == "mysql_position":
        from .MySQL import PositionDB
        return PositionDB
    elif driver.lower() == "postgresql":
        from .PostgreSQL import ChainDB
        return ChainDB(**kwargs)
    elif driver.lower().replace(" ", "") == "sqlserver":
        from .SQLServer import ChainDB
        return ChainDB(**kwargs)
    elif driver.lower() == "sqlite":
        from .SQLite import ChainDB
        return ChainDB(**kwargs)
    elif driver.lower() == "mongodb":
        from .MongoDB import ChainDB
        return ChainDB(**kwargs)
    else:
        raise ValueError("Init saiorm with wrong database driver type")
