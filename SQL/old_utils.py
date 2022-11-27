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
        value_type = eval(value)
        if expression == "equals":
            if type(value_type) == int or type(value_type) == float:
                filterBuilder += f'== {value} '
            else:
                filterBuilder += f'== "{value}" '
        elif expression == "greaterThan":
            if type(value_type) == int or type(value_type) == float:
                filterBuilder += f'> {value} '
            else:
                filterBuilder += f'> "{value}" '
        elif expression == "greaterThanEqualTo":
            if type(value_type) == int or type(value_type) == float: 
                filterBuilder += f'>= {value} '
            else:
                filterBuilder += f'>= "{value}" '
        elif expression == "lessThan":
            if type(value_type) == int or type(value_type) == float:
                filterBuilder += f'< {value} '
            else:
                filterBuilder += f'< "{value}" '
        elif expression == "lessThanEqualTo":
            if type(value_type) == int or type(value_type) == float:
                filterBuilder += f'<= {value} '
            else:
                filterBuilder += f'<= "{value}" '
        elif expression == "notEqualTo":
            if type(value_type) == int or type(value_type) == float:
                filterBuilder += f'!= {value} '
            else:
                filterBuilder += f'!= "{value}" '

        if i == len(columnFilters) - 1:
            break 
        
        filterBuilder += f'{condition} '

    return filterBuilder




def agg_function_helper(query, df, filter_query):

    aggregatorFunction = query['aggregatorFunction']
    aggregatorColumn = query['aggregatorColumn']
    columnFilters = query['columnFilters']
    displayColumns = query['displayColumns']
    groupedBy = query['groupedBy']

    if aggregatorFunction == 'SUM':
        subset = df.query(filter_query)[displayColumns].groupby(groupedBy).sum(aggregatorColumn).reset_index()
        return subset

    if aggregatorFunction == "MAX":
        subset = df.query(filter_query)[displayColumns].groupby(groupedBy).max(aggregatorColumn).reset_index()
        return subset 
    
    if aggregatorFunction == "MIN":
        subset = df.query(filter_query)[displayColumns].groupby(groupedBy).min(aggregatorColumn).reset_index()
        return subset 

    if aggregatorFunction == "AVG":
        subset = df.query(filter_query)[displayColumns].reset_index()
        return subset 

    if aggregatorFunction == "COUNT":
        # consider only the columns that have values > 0 or are not null 
        print(df[aggregatorColumn].dtype)
        if df[aggregatorColumn].dtype == int or df[aggregatorColumn].dtype == float:
            df = df[df[aggregatorColumn] > 0]
        elif df[aggregatorColumn].dtype == object:
            df = df[df[aggregatorColumn].notnull()]

        subset = df.query(filter_query)[displayColumns].groupby(groupedBy)[aggregatorColumn].count().reset_index()
        return subset