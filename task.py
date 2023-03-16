# Description: This file contains the ETL pipeline for the Netflix data
# Author: Alex Thomas
# Date: 2022-03-16
# Version: 1.0
#
# To run this script, you must have the following environment variables set:
# DB_NAME=<your database name>
# DB_USER=<your username>
# DB_PASS=<your password>
# DB_HOST=<your host ip>
# Command to run: python task.py -i <input file>

import pandas as pd
import gender_guesser.detector as gender
from sqlalchemy import create_engine
import logging
import os
import queries
import calendar
import argparse


DEBUG = os.getenv("DEBUG", "False").lower() == "true"
DEBUG = bool(DEBUG)

database_name = os.getenv('DB_NAME')
database_user = os.getenv('DB_USER')
database_pass = os.getenv('DB_PASS')
database_host = os.getenv('DB_HOST')

connection_string = 'postgresql://{}:{}@{}:5432/{}'.format(
    database_user, database_pass, database_host, database_name)

d = gender.Detector(case_sensitive=False)

#
# Utility functions
#



# gender provides an approximation of the gender of a name
def gender(val):
    ret = d.get_gender(val)
    if ret == 'mostly_male':
        return 'male'
    elif ret == 'mostly_female':
        return 'female'
    elif ret == 'andy':
        return 'unknown'
    return ret


# nsplit splits a string into two parts at the first space
def nsplit(val):
    try:
        idx = val.index(' ')
    except ValueError:
        return (val, '')
    return (val[:idx], val[idx+1:])


#
# ETL functions
#


# extract reads a csv file from a given path
def extract(val):
    return pd.read_csv(val)


# transform takes a dataframe and returns a tuple of dataframes
def transform(shows):
    logging.debug('preprocessing csv')
    shows['cast'] = shows['cast'].str.split(',')
    shows['director'] = shows['director'].str.split(',')
    shows['listed_in'] = shows['listed_in'].str.split(',')
    shows['date_added'] = pd.to_datetime(shows['date_added'])

    logging.debug('exploding rows')
    dexplod = shows.explode('director')
    cexplod = shows.explode('cast')
    texplod = shows.explode('listed_in')

    dexplod['director'] = dexplod['director'].str.strip()
    cexplod['cast'] = cexplod['cast'].str.strip()

    logging.debug('dropping duplicates in personnel')
    directors = dexplod['director'].drop_duplicates().dropna()
    actors = cexplod['cast'].drop_duplicates().dropna()

    # Personnel Table
    logging.debug('unifying personnel')
    personnel = pd.concat([actors, directors])
    temp = personnel.reset_index()
    personnel = temp.rename(columns={0: 'name'})
    personnel.drop(columns=['index'], inplace=True)
    personnel.drop_duplicates('name', inplace=True)
    personnel['id'] = range(1, len(personnel) + 1)

    logging.debug('generating gender and splitting names')
    personnel[['first_name', 'last_name']] = personnel['name'].apply(
        nsplit).apply(pd.Series)
    personnel['gender'] = personnel['first_name'].apply(
        str.capitalize).apply(gender)

    # Movie-Personnel Normalisation
    logging.debug('generating cast normalisation')
    cast = {'show_id': cexplod['show_id'], 'name': cexplod['cast']}
    cast = pd.DataFrame(cast)
    cast.dropna(inplace=True)
    cast = cast.assign(personnel_type='cast')

    logging.debug('generating director normalisation')
    director = {'show_id': dexplod['show_id'], 'name': dexplod['director']}
    director = pd.DataFrame(director)
    director.dropna(inplace=True)
    director = director.assign(personnel_type='director')

    logging.debug('joining on names')
    movie_crew = pd.concat([cast, director], axis=0)
    movie_crew = pd.merge(
        personnel[['id', 'name']], movie_crew, on='name', how='left')
    movie_crew.dropna(inplace=True)
    logging.debug('cleaning up')
    movie_crew.rename(columns={'id': 'personnel_id'}, inplace=True)
    movie_crew.drop(columns=['name'], inplace=True)

    # Listings table
    logging.debug('generating listings table')
    listings = {'show_id': texplod['show_id'], 'listing': texplod['listed_in']}
    listings = pd.DataFrame(listings)

    # Drop unnecessary columns from shows
    logging.debug('dropping unnecessary columns')
    shows.drop(columns=['director', 'cast', 'listed_in'], inplace=True)

    return (shows, personnel, movie_crew, listings)


# load takes a tuple of dataframes and loads them into the database
def load(shows, personnel, movie_crew, listings):
    engine = create_engine(connection_string)

    conn = engine.connect()

    conn.execute(queries.create_shows)
    conn.execute(queries.create_personnel)
    conn.execute(queries.create_movie_crew)
    conn.execute(queries.create_listings)

    logging.debug('loading shows table')
    shows.to_sql(name='shows', con=engine, if_exists='append', index=False)

    logging.debug('loading personnel table')
    personnel.to_sql(name='personnel', con=engine,
                     if_exists='append', index=False)

    logging.debug('loading movie_crew table')
    movie_crew.to_sql(name='movie_crew', con=engine,
                      if_exists='append', index=False)

    logging.debug('loading listings table')
    listings.to_sql(name='listings', con=engine,
                    if_exists='append', index=False)


# purge drops all tables in the database
def purge():
    engine = create_engine(connection_string)

    logging.debug('cleaning up previous runs')
    conn = engine.connect()
    table_name = ['shows', 'personnel', 'movie_crew', 'listings', 'history']
    for table in table_name:
        conn.execute('DROP TABLE IF EXISTS {} CASCADE'.format(table))


# execute_sql runs the queries in queries.py
def execute_sql():
    engine = create_engine(connection_string)
    conn = engine.connect()

    result = conn.execute(queries.query4_1_1)
    logging.warning(
        'Number of shows with no listed crew : {}'.format(result.fetchone()[0]))

    result = conn.execute(queries.query4_1_2)
    logging.warning(
        'Number of shows with no listed listings : {}'.format(result.fetchone()[0]))

    result = conn.execute(queries.query5_1_1)
    logging.info('Most popular name for actresses : {}'.format(
        result.fetchone()[0]))

    result = conn.execute(queries.query5_1_2)
    logging.info('Most popular name for actors : {}'.format(
        result.fetchone()[0]))

    result = conn.execute(queries.query5_1_3)
    logging.info('Most popular name for folks whose name could not be reliably gendered or is androgenous : {}'.format(
        result.fetchone()[0]))

    result = conn.execute(queries.query5_2)
    logging.info('The movie that had the longest timespan from release to appearing on Netflix : {}'.format(
        result.fetchone()[0]))

    result = conn.execute(queries.query5_3)
    val = result.fetchone()
    logging.info('The month with the most number of releases historically is {} with {} releases'.format(
        calendar.month_name[int(val[0])], val[1]))

    result = conn.execute(queries.query5_4)
    val = result.fetchone()
    logging.info('The year with the greatest YoY growth in number of releases is {} with {}% growth'.format(
        val[0], val[3]))

    result = conn.execute(queries.query5_5_1.format('Woody Harrelson'))
    val = result.fetchall()
    movie_list = [x[0] for x in val]
    result = conn.execute(queries.query5_5_2.format(
        ','.join(['%s']*len(movie_list))), movie_list)
    val = result.fetchall()
    actresses = [x for x in val if x[1] > 1]
    logging.info('The actresses who have worked with Woodie Harrelson more than once are : {}'.format(
        ', '.join([x[0] for x in actresses])))


# check_history checks if the csv has been processed before
def check_history(name):
    engine = create_engine(connection_string)
    conn = engine.connect()
    conn.execute(queries.create_history)
    result = conn.execute(queries.check_history.format(name)).fetchone()
    if result != None:
        return True
    else:
        conn.execute(queries.insert_history.format(name))
        return False

#
# test functions
# These functions test the various functions in the ETL pipeline
#


def test_check_history():
    purge()
    assert check_history('netflix_titles.csv') == False
    assert check_history('netflix_titles.csv') == True


def test_extract():
    shows = extract('netflix_titles.csv')
    assert shows.shape == (7787, 12)


def test_transform():
    shows = extract('netflix_titles.csv')
    shows, personnel, movie_crew, listings = transform(shows)
    assert shows.shape == (7787, 9)


def test_load():
    shows = extract('netflix_titles.csv')
    shows, personnel, movie_crew, listings = transform(shows)
    load(shows, personnel, movie_crew, listings)
    engine = create_engine(connection_string)
    conn = engine.connect()
    result = conn.execute('SELECT COUNT(*) FROM shows').fetchone()
    assert result[0] == 7787


def main():
    parser = argparse.ArgumentParser(description='ETL for Netflix data')
    parser.add_argument('-i', '--input', type=str, help='Input CSV name')
    args = parser.parse_args()

    if DEBUG == True:
        logging.basicConfig(level=logging.DEBUG)
        logging.info('debug mode enabled')
        logging.debug('purging previous runs')
        purge()
    else:
        logging.basicConfig(level=logging.INFO)

    if not check_history(args.input):
        logging.info('extracting csv')
        shows = extract(args.input)

        logging.info('performing transforms')
        shows, personnel, movie_crew, listings = transform(shows)

        logging.info('loading to database')
        load(shows, personnel, movie_crew, listings)

    else:
        logging.info('skipping extract, transform and load')

    logging.info('executing sql queries')
    execute_sql()


main()
