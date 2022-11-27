# This file contains all the helper functions needed in search.py 

def buildFilterQuery(columnFilters):

    if not columnFilters:
        return 'ilevel_0 in ilevel_0'


    filterBuilder = ""
    for i, filter_object in enumerate(columnFilters):

        filter_col = list(filter_object.keys())[0]
        filterBuilder += f'`{filter_col}` '

        filter_col_val = filter_object[filter_col]
        keys = list(filter_col_val.keys())
        expression = keys[0]
        if len(keys) > 1:
            condition = filter_col_val[keys[1]]
        value = filter_col_val[expression]
        try:
            value = int(value)
        except:
            pass
        print(type(value))
        if expression == "equals":
            if type(value) == int:
                filterBuilder += f'== {value} '
            else:
                filterBuilder += f'== "{value}" '
        elif expression == "greaterThan":
            if type(value) == int:
                filterBuilder += f'> {value} '
            else:
                filterBuilder += f'> "{value}" '
        elif expression == "greaterThanEqualTo":
            if type(value) == int:
                filterBuilder += f'>= {value} '
            else:
                filterBuilder += f'>= "{value}" '
        elif expression == "lessThan":
            if type(value) == int:
                filterBuilder += f'< {value} '
            else:
                filterBuilder += f'< "{value}" '
        elif expression == "lessThanEqualTo":
            if type(value) == int:
                filterBuilder += f'<= {value} '
            else:
                filterBuilder += f'<= "{value}" '
        elif expression == "notEqualTo":
            if type(value) == int:
                filterBuilder += f'!= {value} '
            else:
                filterBuilder += f'!= "{value}" '

        if i == len(columnFilters) - 1:
            break 
        
        filterBuilder += f'{condition} '

    return filterBuilder


def queryDecider(expression):

    if expression == "greaterThan":
        exp = ">"
        return exp


def agg_function_helper(query, df, filter_query):

    aggregatorFunction = query['aggregatorFunction']
    aggregatorColumn = query['aggregatorColumn']
    columnFilters = query['columnFilters']
    displayColumns = query['displayColumns']
    groupedBy = query['groupedBy']
    having = query['having']

    if aggregatorFunction == 'SUM':
        subset = df.query(filter_query)[displayColumns].groupby(groupedBy).sum(aggregatorColumn).reset_index()
        return subset

    if aggregatorFunction == "MAX":
        subset = df.query(filter_query)[displayColumns].groupby(groupedBy).max(aggregatorColumn).reset_index()
        return subset 