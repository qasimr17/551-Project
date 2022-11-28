import mysql.connector
import pandas as pd
import streamlit as st 
from datetime import datetime
from sqlalchemy import create_engine
import commands 
import json
# import old_utils 
import utils
import sys
import numpy as np
# Parse the user-returned parameters here. 


def reduceSelect(subsets, distinctCols, message):
    """ This reducer function simply takes in inputs from all partitions and concatenates them.
    Mimicking the reduce function for an SQL query."""
    intermediate_dict = {}
    if distinctCols:
        # return pd.concat(subsets, ignore_index=True).drop_duplicates(subset=distinctCols)
        final_val = pd.concat(subsets, ignore_index=True).drop_duplicates(subset=distinctCols)
        intermediate_dict['message'] = "The final, complete df is just a concatenation of these partition results."
        intermediate_dict['value'] = final_val.to_json(orient='table')
        message['final_result'] = intermediate_dict
        return message
    else:
        final_val = pd.concat(subsets, ignore_index=True)
        intermediate_dict['message'] = "The final, complete df is just a concatenation of these partition results."
        intermediate_dict['value'] = final_val.to_json(orient='table')
        message['final_result'] = intermediate_dict
        return message

def reduceSelectAggregator(subsets, aggregator, distinctCols, message):

    intermediate_dict = {}
    if aggregator == "MAX":
        final_val = pd.concat(subsets, ignore_index=True).max()
        intermediate_dict['message'] = "The final output is a max of the intermediate results: "
        intermediate_dict['value'] = str(final_val)
        message['final_result'] = intermediate_dict
        return message

    elif aggregator == "MIN":
        final_val = pd.concat(subsets, ignore_index=True).min()
        intermediate_dict['message'] = "The final output is a min of the intermediate results: "
        intermediate_dict['value'] = str(final_val)
        message['final_result'] = intermediate_dict
        return message

    elif aggregator == "SUM":
        final_val = pd.concat(subsets, ignore_index=True).sum()
        intermediate_dict['message'] = "The final output is also a sum of the intermediate results: "
        intermediate_dict['value'] = str(final_val)
        message['final_result'] = intermediate_dict
        return message

    elif aggregator == "AVG":
        total, length = 0, 0 
        for x, y in subsets:
            total += x * y 
            length += y 
        final_val = round(total / length, 3)
        intermediate_dict['message'] = "For the final output, we compute the overall_total and overall_count to get: "
        intermediate_dict['value'] = str(final_val)
        message['final_result'] = intermediate_dict
        return message

    elif aggregator == "COUNT":
        if not distinctCols:
            total_count = sum(subsets)
            intermediate_dict['message'] = "The final output is a sum of all the intermediate counts: "
            intermediate_dict['value'] = str(total_count)
            message['final_result'] = intermediate_dict
            return message
            # return total_count 

        if distinctCols:
            subsets = np.array([item for sublist in subsets for item in sublist], dtype=object)
            unique_len = len(np.unique(subsets))
            # print(f"The count of unique values is thus: {unique_len}")
            intermediate_dict['message'] = "To compute the final result, we found all the distinct values in each partition, sent them to the reducer function and then computed the number of total unique values."
            intermediate_dict['value'] = str(unique_len)
            message['final_result'] = intermediate_dict
            return message
            # return unique_len


def reduceSum(subsets, displayColumns, groupedBy, aggregatorColumn, message):

    intermediate_dict = {}
    total_df = pd.concat(subsets)
    total_df = total_df[displayColumns].groupby(groupedBy).sum(aggregatorColumn)
    intermediate_dict['message'] = "The final output is then a concatenation of the groupby of these intermediate results."
    intermediate_dict['value'] = total_df.to_json(orient='table')
    message['final_result'] = intermediate_dict
    return message
    # return total_df


def reduceMax(subsets, displayColumns, groupedBy, aggregatorColumn, message):

    intermediate_dict = {}
    total_df = pd.concat(subsets)
    total_df = total_df[displayColumns].groupby(groupedBy).max(aggregatorColumn)
    intermediate_dict['message'] = "The final output is then a concatenation of the groupby of these intermediate results."
    intermediate_dict['value'] = total_df.to_json(orient='table')
    print(f"Type is: {type(intermediate_dict)}")
    message['final_result'] = intermediate_dict
    return message
    # return total_df

def reduceMin(subsets, displayColumns, groupedBy, aggregatorColumn, message):

    intermediate_dict = {}
    total_df = pd.concat(subsets)
    total_df = total_df[displayColumns].groupby(groupedBy).min(aggregatorColumn)
    intermediate_dict['message'] = "The final output is then a concatenation of the groupby of these intermediate results."
    intermediate_dict['value'] = total_df.to_json(orient='table')
    message['final_result'] = intermediate_dict
    return message
    # return total_df

def reduceAvg(subsets, displayColumns, groupedBy, aggregatorColumn, message):

    intermediate_dict = {}
    total_df = pd.concat(subsets)
    total_df = total_df[displayColumns].groupby(groupedBy).mean(aggregatorColumn)
    intermediate_dict['message'] = "The final output is then a concatenation of the groupby of these intermediate results."
    intermediate_dict['value'] = total_df.to_json(orient='table')
    message['final_result'] = intermediate_dict
    return message
    # return total_df

def reduceCount(subsets, displayColumns, groupedBy, aggregatorColumn, message):

    intermediate_dict = {}
    total_df = pd.concat(subsets)
    total_df = total_df[displayColumns].groupby(groupedBy).sum(aggregatorColumn)
    intermediate_dict['message'] = "The final output is then a concatenation of the groupby of these intermediate results."
    intermediate_dict['value'] = total_df.to_json(orient='table')
    message['final_result'] = intermediate_dict
    return message
    # return total_df

def call_relevant_reducer(query, reducer_list, message):
    """ Takes in the json query and list of reduced paritions and applies
    the relevant reducer functions on it."""

    aggregatorFunction = query['aggregatorFunction']
    aggregatorColumn = query['aggregatorColumn']
    displayColumns = query['displayColumns']
    groupedBy = query['groupedBy']

    if aggregatorFunction == "SUM":
        return reduceSum(reducer_list, displayColumns, groupedBy, aggregatorColumn, message)

    elif aggregatorFunction == "MAX":
        return reduceMax(reducer_list, displayColumns, groupedBy, aggregatorColumn, message)

    elif aggregatorFunction == "MIN":
        return reduceMin(reducer_list, displayColumns, groupedBy, aggregatorColumn, message)

    elif aggregatorFunction == "AVG":
        return reduceAvg(reducer_list, displayColumns, groupedBy, aggregatorColumn, message) 

    elif aggregatorFunction == "COUNT":
        return reduceCount(reducer_list, displayColumns, groupedBy, aggregatorColumn, message) 


def agg_function(query):

    """ This function is run if a user calls an aggregator function such as COUNT, SUM, MIN, MAX, AVG.
    If an aggregator function is supplied, then the user must specify a column for the data to be grouped by as well."""

    filePath = query['filePath']
    columnFilters = query['columnFilters']

    # tableName, partitions = commands.commands_main('getPartitionLocations', path = filePath)
    partitions = commands.commands_main('getPartitionLocations', path = filePath)['partitions']

    reducer = []
    intermediate_message_list = []
    total_json = {}
    filterQuery = utils.buildFilterQuery(columnFilters=columnFilters)

    for partition in partitions:
        df = commands.commands_main('readPartition', filePath, partition)
        df = pd.read_json(df, orient='table')
        subset = utils.agg_function_helper(query=query, df=df, filter_query=filterQuery)
        # new 
        intermediate_message = utils.buildMessageAggregator(subset, partition)
        intermediate_message_list.append(intermediate_message)
        reducer.append(subset)

    total_json['intermediate_message'] = intermediate_message_list
    reduced = call_relevant_reducer(query=query, reducer_list=reducer, message=total_json)
    reduced = json.dumps(reduced)

    return reduced
    # return reduced


def select(query):

    """ This function receives simply a SELECT statement (which could be followed by other filters such as
    groupBy and where which will have to be made more dynamic."""

    # Take example of SELECT first_name, second_name, goals_scored FROM players.csv WHERE first_name = 'Ben' 
    filePath = query['filePath']
    columnFilters = query['columnFilters']
    displayColumns = query['displayColumns']
    distinctCols = query['distinctCols']

    # tableName, partitions = commands.commands_main('getPartitionLocations', path = filePath)
    partitions = commands.commands_main('getPartitionLocations', path = filePath)['partitions']

    reducer = []
    intermediate_message_list = []
    total_json = {}
    filterQuery = utils.buildFilterQuery(columnFilters=columnFilters)

    for partition in partitions:
        df = commands.commands_main('readPartition', filePath, partition)
        df = pd.read_json(df, orient='table')
        if distinctCols:
            subset = df.query(filterQuery)[displayColumns].drop_duplicates(subset=distinctCols)
            intermediate_message = utils.buildMessageSelect(subset.head(3), partition, distinctCols)
        else:
            subset = df.query(filterQuery)[displayColumns]
            intermediate_message = utils.buildMessageSelect(subset.head(3), partition, distinctCols)

        intermediate_message_list.append(intermediate_message)
        reducer.append(subset)

    total_json['intermediate_message'] = intermediate_message_list
    reduced_select = reduceSelect(reducer, distinctCols=distinctCols, message=total_json)

    reduced_select = json.dumps(reduced_select)
    return reduced_select


def select_with_aggregator(query):

    """ This function receives simply a SELECT statement (which could be followed by other filters such as
    groupBy and where which will have to be made more dynamic."""

    # Take example of SELECT first_name, second_name, goals_scored FROM players.csv WHERE first_name = 'Ben' 

    aggregatorFunction = query['aggregatorFunction']
    filePath = query['filePath']
    columnFilters = query['columnFilters']
    displayColumns = query['displayColumns']
    distinctCols = query['distinctCols']

    # tableName, partitions = commands.commands_main('getPartitionLocations', path = filePath)
    partitions = commands.commands_main('getPartitionLocations', path = filePath)['partitions']

    reducer = []
    total_json = {}
    intermediate_message_list = []

    filterQuery = utils.buildFilterQuery(columnFilters=columnFilters)

    for partition in partitions:
        df = commands.commands_main('readPartition', filePath, partition)
        df = pd.read_json(df, orient='table')

        if aggregatorFunction == "MAX":
            subset = df.query(filterQuery)[displayColumns].max()
            value = subset.values[0]
            intermediate_message = utils.buildMessageSelectAggregator(value, aggregatorFunction, partition)

        elif aggregatorFunction == "MIN":
            subset = df.query(filterQuery)[displayColumns].min()
            value = subset.values[0]
            intermediate_message = utils.buildMessageSelectAggregator(value, aggregatorFunction, partition)

        elif aggregatorFunction == "AVG":
            
            intermediate_avg = df.query(filterQuery)[displayColumns].mean()
            intermediate_avg = round(intermediate_avg.values[0], 3)
            items = len(df)
            subset = intermediate_avg, items 
            intermediate_message = utils.buildMessageSelectAggregator(subset, aggregatorFunction, partition)

        elif aggregatorFunction == "COUNT":
            if not distinctCols:
                intermediate_count = df.query(filterQuery)[displayColumns].count()
                intermediate_count = intermediate_count.values[0]
                subset = intermediate_count
                intermediate_message = utils.buildMessageSelectAggregator(subset, aggregatorFunction, partition)
            else:
                intermediate_values = df.query(filterQuery)[displayColumns]
                col = displayColumns[0]
                intermediate_values = eval(f"intermediate_values.{col}.unique()")
                subset = intermediate_values
                intermediate_message = utils.buildMessageSelectAggregator(subset, aggregatorFunction, partition, distinctCols = distinctCols)

        elif aggregatorFunction == "SUM":
                subset = df.query(filterQuery)[displayColumns].sum()
                value = subset.values[0]
                intermediate_message = utils.buildMessageSelectAggregator(value, aggregatorFunction, partition)

        intermediate_message_list.append(intermediate_message)
        reducer.append(subset)

    total_json['intermediate_message'] = intermediate_message_list
    reduced_select = reduceSelectAggregator(reducer, aggregator=aggregatorFunction, distinctCols=distinctCols, message=total_json)
    
    reduced_select = json.dumps(reduced_select)
    return reduced_select


def main(query):

    """ Receives a query from the user in JSON format relating to a search and analytics query that
    the user runs. The function then parses the received JSON and calls the relevant columns thereafter."""

    aggregatorFunction = query['aggregatorFunction']
    groupedBy = query['groupedBy']

    if not groupedBy:
        # User just wants to display the relevant columns 
        if aggregatorFunction:
            return select_with_aggregator(query=query) 
        else:
            return select(query=query) 

    else:
        return agg_function(query=query)



# file = './test_query.json'
# with open(file, 'r') as f:
#     query = json.load(f)

# print(main(query))
        

