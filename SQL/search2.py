import mysql.connector
import pandas as pd
import streamlit as st 
from datetime import datetime
from sqlalchemy import create_engine
import commands 
import json
import utils2
import sys

# Parse the user-returned parameters here. 


def reduceSelect(subsets, distinctCols):
    """ This reducer function simply takes in inputs from all partitions and concatenates them.
    Mimicking the reduce function for an SQL query."""
    if distinctCols:
        return pd.concat(subsets, ignore_index=True).drop_duplicates(subset=distinctCols)
    else:
        return pd.concat(subsets, ignore_index=True)

def reduceSelectAggregator(subsets, aggregator):

    if aggregator == "MAX":
        return pd.concat(subsets, ignore_index=True).max()
    elif aggregator == "MIN":
        return pd.concat(subsets, ignore_index=True).min()
    elif aggregator == "AVG":
        return pd.concat(subsets, ignore_index=True).mean()

def reduceSum(subsets, displayColumns, groupedBy, aggregatorColumn, having):

    total_df = pd.concat(subsets)
    if not having: 
        total_df = total_df[displayColumns].groupby(groupedBy).sum(aggregatorColumn)
    if having:
        expression = list(having.keys())[0]
        exp = utils2.queryDecider(expression)
        value = having[expression]
        lambda_exp = eval(f"lambda x: x.{aggregatorColumn}.sum() {exp} {value}")
        print(lambda_exp)
        total_df = total_df[displayColumns].groupby(groupedBy).filter(lambda_exp).groupby(groupedBy).sum(aggregatorColumn)
    return total_df


def reduceMax(subsets, displayColumns, groupedBy, aggregatorColumn):

    total_df = pd.concat(subsets)
    total_df = total_df[displayColumns].groupby(groupedBy).max(aggregatorColumn)
    return total_df

def call_relevant_reducer(query, reducer_list):
    """ Takes in the json query and list of reduced paritions and applies
    the relevant reducer functions on it."""

    aggregatorFunction = query['aggregatorFunction']
    aggregatorColumn = query['aggregatorColumn']
    displayColumns = query['displayColumns']
    groupedBy = query['groupedBy']
    having = query['having']

    if aggregatorFunction == "SUM":
        return reduceSum(reducer_list, displayColumns, groupedBy, aggregatorColumn, having=having)

    elif aggregatorFunction == "MAX":
        return reduceMax(reducer_list, displayColumns, groupedBy, aggregatorColumn, having=having)

def agg_function(query):

    """ This function is run if a user calls an aggregator function such as COUNT, SUM, MIN, MAX, AVG.
    If an aggregator function is supplied, then the user must specify a column for the data to be grouped by as well."""

    filePath = query['filePath']
    columnFilters = query['columnFilters']

    tableName, partitions = commands.main('getPartitionLocations', path = filePath, search=True)

    reducer = []

    for partition in partitions:
        df = commands.main('readPartition', filePath, partition, search=True)
        filterQuery = utils2.buildFilterQuery(columnFilters=columnFilters)
        subset = utils2.agg_function_helper(query=query, df=df, filter_query=filterQuery)
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

    tableName, partitions = commands.main('getPartitionLocations', path = filePath, search=True)

    reducer = []

    for partition in partitions:
        df = commands.main('readPartition', filePath, partition, search=True)
        filterQuery = utils2.buildFilterQuery(columnFilters=columnFilters)
        if distinctCols:
            subset = df.query(filterQuery)[displayColumns].drop_duplicates(subset=distinctCols)
        else:
            subset = df.query(filterQuery)[displayColumns]

        reducer.append(subset)

    reduced_select = reduceSelect(reducer, distinctCols=distinctCols)
    print(reduced_select)


def select_with_aggregator(query):

    """ This function receives simply a SELECT statement (which could be followed by other filters such as
    groupBy and where which will have to be made more dynamic."""

    # Take example of SELECT first_name, second_name, goals_scored FROM players.csv WHERE first_name = 'Ben' 

    aggregatorFunction = query['aggregatorFunction']
    filePath = query['filePath']
    columnFilters = query['columnFilters']
    displayColumns = query['displayColumns']
    distinctCols = query['distinctCols']

    tableName, partitions = commands.main('getPartitionLocations', path = filePath, search=True)

    reducer = []
    filterQuery = utils2.buildFilterQuery(columnFilters=columnFilters)

    for partition in partitions:
        df = commands.main('readPartition', filePath, partition, search=True)

        if aggregatorFunction == "MAX":
            subset = df.query(filterQuery)[displayColumns].max()
        elif aggregatorFunction == "MIN":
            subset = df.query(filterQuery)[displayColumns].min()
        elif aggregatorFunction == "AVG":
            subset = df.query(filterQuery)[displayColumns].mean()

        reducer.append(subset)

    reduced_select = reduceSelectAggregator(reducer, aggregator=aggregatorFunction)
    print(reduced_select)

def main(query):

    """ Receives a query from the user in JSON format relating to a search and analytics query that
    the user runs. The function then parses the received JSON and calls the relevant columns thereafter."""

    aggregatorFunction = query['aggregatorFunction']
    groupedBy = query['groupedBy']

    if not groupedBy:
        # User just wants to display the relevant columns 
        if aggregatorFunction:
            select_with_aggregator(query=query) 
        else:
            select(query=query) 

    else:
        agg_function(query=query)



file = sys.argv[1]
with open(file, 'r') as f:
    query = json.load(f)

main(query)
        

