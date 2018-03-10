from .MySQL import *


def init(driver="MySQL", table_name_prefix=""):
    if driver.lower() == "mysql":
        return CoherentDB(table_name_prefix=table_name_prefix)
