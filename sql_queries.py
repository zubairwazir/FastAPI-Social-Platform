from typing import List

def get_tables_string(arr: dict, tname: str):
    def elem_to_str(elem, tname):
        return "\"" + tname + "\"." + "\"" + elem + "\""

    arr_2 = [elem_to_str(elem, tname) for elem in arr]

    result = ", ".join(arr_2)

    return result
