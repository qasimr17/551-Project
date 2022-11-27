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


def reduceSelect(subsets, distinctCols):
    """ This reducer function simply takes in inputs from all partitions and concatenates them.
    Mimicking the reduce function for an SQL query."""
    if distinctCols:
        return pd.concat(subsets, ignore_index=True).drop_duplicates(subset=distinctCols)
    else:
        return pd.concat(subsets, ignore_index=True)

def reduceSelectAggregator(subsets, aggregator, distinctCols):

    if aggregator == "MAX":
        return pd.concat(subsets, ignore_index=True).max()
    elif aggregator == "MIN":
        return pd.concat(subsets, ignore_index=True).min()
    elif aggregator == "AVG":
        # new attempt:
        total, length = 0, 0 
        for x, y in subsets:
            total += x * y 
            length += y 
        print(f"The final answer is: {total/length}")
        return total / length
        # return pd.concat(subsets, ignore_index=True).mean()
    elif aggregator == "COUNT":
        if not distinctCols:
            total_count = sum(subsets)
            print(f"The total count, after summing all the counts from each partition is: {total_count}.")
            return total_count 

        if distinctCols:
            subsets = np.array([item for sublist in subsets for item in sublist], dtype=object)
            unique_len = len(np.unique(subsets))
            print(f"The count of unique values is thus: {unique_len}")
            return unique_len


def reduceSum(subsets, displayColumns, groupedBy, aggregatorColumn):

    total_df = pd.concat(subsets)
    total_df = total_df[displayColumns].groupby(groupedBy).sum(aggregatorColumn)
    return total_df


def reduceMax(subsets, displayColumns, groupedBy, aggregatorColumn):

    total_df = pd.concat(subsets)
    total_df = total_df[displayColumns].groupby(groupedBy).max(aggregatorColumn)
    return total_df

def reduceMin(subsets, displayColumns, groupedBy, aggregatorColumn):

    total_df = pd.concat(subsets)
    total_df = total_df[displayColumns].groupby(groupedBy).min(aggregatorColumn)
    return total_df

def reduceAvg(subsets, displayColumns, groupedBy, aggregatorColumn):

    total_df = pd.concat(subsets)
    total_df = total_df[displayColumns].groupby(groupedBy).mean(aggregatorColumn)
    return total_df

def reduceCount(subsets, displayColumns, groupedBy, aggregatorColumn):

    total_df = pd.concat(subsets)
    total_df = total_df[displayColumns].groupby(groupedBy).sum(aggregatorColumn)
    return total_df

def call_relevant_reducer(query, reducer_list):
    """ Takes in the json query and list of reduced paritions and applies
    the relevant reducer functions on it."""

    aggregatorFunction = query['aggregatorFunction']
    aggregatorColumn = query['aggregatorColumn']
    displayColumns = query['displayColumns']
    groupedBy = query['groupedBy']

    if aggregatorFunction == "SUM":
        return reduceSum(reducer_list, displayColumns, groupedBy, aggregatorColumn)

    elif aggregatorFunction == "MAX":
        return reduceMax(reducer_list, displayColumns, groupedBy, aggregatorColumn)

    elif aggregatorFunction == "MIN":
        return reduceMin(reducer_list, displayColumns, groupedBy, aggregatorColumn)

    elif aggregatorFunction == "AVG":
        return reduceAvg(reducer_list, displayColumns, groupedBy, aggregatorColumn) 

    elif aggregatorFunction == "COUNT":
        return reduceCount(reducer_list, displayColumns, groupedBy, aggregatorColumn) 

def agg_function(query):

    """ This function is run if a user calls an aggregator function such as COUNT, SUM, MIN, MAX, AVG.
    If an aggregator function is supplied, then the user must specify a column for the data to be grouped by as well."""

    filePath = query['filePath']
    columnFilters = query['columnFilters']

    # tableName, partitions = commands.commands_main('getPartitionLocations', path = filePath)
    partitions = commands.commands_main('getPartitionLocations', path = filePath)['partitions']

    reducer = []
    filterQuery = utils.buildFilterQuery(columnFilters=columnFilters)

    for partition in partitions:
        df = commands.commands_main('readPartition', filePath, partition)
        df = pd.read_json(df, orient='table')
        subset = utils.agg_function_helper(query=query, df=df, filter_query=filterQuery)
        reducer.append(subset)

    reduced = call_relevant_reducer(query=query, reducer_list=reducer)
    print(reduced)


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
    filterQuery = utils.buildFilterQuery(columnFilters=columnFilters)

    for partition in partitions:
        df = commands.commands_main('readPartition', filePath, partition)
        df = pd.read_json(df, orient='table')
        if distinctCols:
            subset = df.query(filterQuery)[displayColumns].drop_duplicates(subset=distinctCols)
        else:
            subset = df.query(filterQuery)[displayColumns]

        reducer.append(subset)

    reduced_select = reduceSelect(reducer, distinctCols=distinctCols)

    return reduced_select.to_json(orient='table')
    # return reduced_select.to_json(orient='table')

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
    filterQuery = utils.buildFilterQuery(columnFilters=columnFilters)

    for partition in partitions:
        df = commands.commands_main('readPartition', filePath, partition)
        df = pd.read_json(df, orient='table')

        if aggregatorFunction == "MAX":
            subset = df.query(filterQuery)[displayColumns].max()
        elif aggregatorFunction == "MIN":
            subset = df.query(filterQuery)[displayColumns].min()
        elif aggregatorFunction == "AVG":
            
            intermediate_avg = df.query(filterQuery)[displayColumns].mean()
            intermediate_avg = intermediate_avg.values[0]
            items = len(df)
            subset = intermediate_avg, items 
            # Need to send this as json for each partition for front-end
            print(f"The average value of {displayColumns[0]} for partition {partition} is: {intermediate_avg} using {items} items.")
        
        elif aggregatorFunction == "COUNT":
            if not distinctCols:
                intermediate_count = df.query(filterQuery)[displayColumns].count()
                intermediate_count = intermediate_count.values[0]
                print(f"The count of {displayColumns[0]} in partition {partition} is {intermediate_count}.")
                subset = intermediate_count
            else:
                intermediate_values = df.query(filterQuery)[displayColumns]
                col = displayColumns[0]
                intermediate_values = eval(f"intermediate_values.{col}.unique()")
                # print(f"The unique values for {displayColumns} in partition {partition} are {intermediate_values}.")
                subset = intermediate_values
            
        reducer.append(subset)

    reduced_select = reduceSelectAggregator(reducer, aggregator=aggregatorFunction, distinctCols=distinctCols)
    # print(reduced_select)
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
        agg_function(query=query)



file = './test_query.json'
with open(file, 'r') as f:
    query = json.load(f)

print(main(query))
        

