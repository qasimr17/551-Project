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


def buildMessageSelectAggregator(value, aggregatorFunction, partition, distinctCols = None):

    intermediate_message = {}
    if aggregatorFunction == "MAX":
        intermediate_message['message'] = f"The max value for partition {partition} is: "
        intermediate_message['value'] = f'{value}'
        return intermediate_message

    if aggregatorFunction == "MIN":
        intermediate_message['message'] = f"The min value for partition {partition} is: "
        intermediate_message['value'] = f'{value}'
        return intermediate_message

    if aggregatorFunction == "AVG":
        intermediate_message['message'] = f"The avg value and items used for partition {partition} are: "
        intermediate_message['value'] = f'Average: {value[0]}, Items: {value[1]}'
        return intermediate_message

    if aggregatorFunction == "COUNT":
        if not distinctCols:
            intermediate_message['message'] = f"The counts in partition {partition} are: "
            intermediate_message['value'] = f'{value}'
            return intermediate_message
        else:
            # intermediate_message['message'] = f"The values in partition {partition}, whose distinct values will be counted in the end are: "
            intermediate_message['message'] = f"The number of distinct values in partition {partition} are: "
            intermediate_message['value'] = f'{len(value)}'
            return intermediate_message     

    if aggregatorFunction == "SUM":
        intermediate_message['message'] = f"The sum for partition {partition} is: "
        intermediate_message['value'] = f'{value}'
        return intermediate_message


def buildMessageSelect(value, partition, distinctCols = None):

    value = value.to_json(orient='table')
    intermediate_message = {}
    intermediate_message['message'] = f"Values for {partition} are: "
    intermediate_message['value'] = f'{value}'
    return intermediate_message

def buildMessageAggregator(value, partition):

    value = value.to_json(orient='table')
    intermediate_message = {}
    intermediate_message['message'] = f"Result for partition {partition} is: "
    intermediate_message['value'] = f'{value}'
    return intermediate_message
