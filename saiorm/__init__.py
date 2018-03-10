#!/usr/bin/env python
# -*- coding:utf-8 -*-


def init(driver="MySQL", table_name_prefix=""):
    if driver.lower() == "mysql":
        from .MySQL import CoherentDB
        return CoherentDB(table_name_prefix=table_name_prefix)
