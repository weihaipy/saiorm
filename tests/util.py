def split_sql_file_into_statements_list(test_sql_file_path):
    """
    split string in sql file to single statement list

    :param test_sql_file_path:
    :return: list
    """
    res = []
    with open(test_sql_file_path, "r", encoding="utf-8") as f:
        statements = f.read()
        statements = statements.replace("\r\n", "\n").replace("\r", "\n")
        lines = statements.split(";\n")
        for line in lines:
            res.append(line)
    return res
