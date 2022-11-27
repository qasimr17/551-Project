def buildFilterQuery(columnFilters):

    if not columnFilters:
        return 'ilevel_0 in ilevel_0'


    filterBuilder = ""
    for i, filter_object in enumerate(columnFilters):
        filter_col = filter_object['column']
        filterBuilder += f'`{filter_col}` '

        expression = filter_object['operator']
        value = filter_object['value']
        condition = filter_object['condition']

        try:
            value = eval(value)
        except:
            pass 
        if expression == "equals":
            if type(value) == int or type(value) == float:
                filterBuilder += f'== {value} '
            else:
                filterBuilder += f'== "{value}" '

        elif expression == "greaterThan":
            if type(value) == int or type(value) == float:
                filterBuilder += f'> {value} '
            else:
                filterBuilder += f'> "{value}" '

        elif expression == "greaterThanEqualTo":
            if type(value) == int or type(value) == float: 
                filterBuilder += f'>= {value} '
            else:
                filterBuilder += f'>= "{value}" '

        elif expression == "lessThan":
            if type(value) == int or type(value) == float:
                filterBuilder += f'< {value} '
            else:
                filterBuilder += f'< "{value}" '

        elif expression == "lessThanEqualTo":
            if type(value) == int or type(value) == float:
                filterBuilder += f'<= {value} '
            else:
                filterBuilder += f'<= "{value}" '

        elif expression == "notEqualTo":
            if type(value) == int or type(value) == float:
                filterBuilder += f'!= {value} '
            else:
                filterBuilder += f'!= "{value}" '

        if i == len(columnFilters) - 1:
            break 

        filterBuilder += f'{condition}'

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
