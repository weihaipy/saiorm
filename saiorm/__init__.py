#!/usr/bin/env python
# -*- coding:utf-8 -*-
from .utility import GraceDict


def init(driver="MySQL", **kwargs):
    driver_lower = driver.lower()
    if driver_lower == "mysql":
        from .MySQL import ChainDB
        return ChainDB(**kwargs)
    elif driver_lower == "mysql_position":
        from .MySQL import PositionDB
        return PositionDB(**kwargs)
    elif driver_lower == "postgresql":
        from .PostgreSQL import ChainDB
        return ChainDB(**kwargs)
    elif driver_lower.replace(" ", "") == "sqlserver":
        from .SQLServer import ChainDB
        return ChainDB(**kwargs)
    elif driver_lower == "sqlite":
        from .SQLite import ChainDB
        return ChainDB(**kwargs)
    elif driver_lower == "mongodb":
        from .MongoDB import ChainDB
        return ChainDB(**kwargs)
    else:
        raise ValueError("saiorm does not support " + driver)
