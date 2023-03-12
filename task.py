import pandas as pd
import gender_guesser.detector as gender
from sqlalchemy import create_engine
import logging
import os

database_name = os.getenv('DB_NAME')
database_user = os.getenv('DB_USER')
database_pass = os.getenv('DB_PASS')
database_host = os.getenv('DB_HOST')

connection_string = 'postgresql://{}:{}@{}:5432/{}'.format(database_user, database_pass, database_host, database_name)
d = gender.Detector(case_sensitive=False)

def gender(val):
    ret = d.get_gender(val)
    if ret == 'mostly_male':
        return 'male'
    elif ret == 'mostly_female':
        return 'female'
    elif ret == 'andy':
        return 'unknown'
    return ret


def nsplit(val):
    try:
        idx = val.index(' ')
    except ValueError:
        return (val, '')
    return (val[:idx], val[idx+1:])


def extract(val):
    return pd.read_csv(val)


def transform(shows):

    logging.info('preprocessing csv')
    shows['cast'] = shows['cast'].str.split(',')
    shows['director'] = shows['director'].str.split(',')
    shows['listed_in'] = shows['listed_in'].str.split(',')
    shows['date_added'] = pd.to_datetime(shows['date_added'])
    
    logging.info('exploding rows')
    dexplod = shows.explode('director')
    cexplod = shows.explode('cast')
    texplod = shows.explode('listed_in')

    logging.info('dropping duplicates in personnel')
    directors = dexplod['director'].drop_duplicates().dropna()
    actors = cexplod['cast'].drop_duplicates().dropna()

    # Personnel Table 
    
    logging.info('unifying personnel')
    personnel = pd.concat([actors, directors])
    temp = personnel.reset_index()
    personnel = temp.rename(columns={0: 'name'})
    personnel.drop(columns=['index'], inplace=True)
    personnel.drop_duplicates('name', inplace=True)
    personnel['id'] = range(1, len(personnel) + 1)

    logging.info('generating gender and splitting names')
    personnel['name'] = personnel['name'].str.strip()
    personnel[['first_name', 'last_name']] = personnel['name'].apply(nsplit).apply(pd.Series)    
    personnel['gender'] = personnel['first_name'].apply(str.capitalize).apply(gender)
    # personnel.drop(columns=['name'], inplace=True)

    # Movie-Personnel Normalisation
    
    logging.info('generating cast normalisation')
    cast = {'show_id': cexplod['show_id'], 'name': cexplod['cast']}
    cast = pd.DataFrame(cast)
    cast.dropna(inplace=True)
    cast = cast.assign(personnel_type='cast')

    logging.info('generating director normalisation')
    director = {'show_id': dexplod['show_id'], 'name': dexplod['director']}
    director = pd.DataFrame(director)
    director.dropna(inplace=True)
    director = director.assign(personnel_type='director')

    logging.info('joining on names')
    movie_crew = pd.concat([cast, director], axis=0)
    movie_crew = pd.merge(personnel[['id', 'name']], movie_crew, on='name', how='inner')
    logging.info('cleaning up')
    movie_crew.rename(columns={'id': 'personnel_id'}, inplace=True)
    movie_crew.drop(columns=['name'], inplace=True)

    # Listings table

    logging.info('generating listings table')
    listings = {'show_id': texplod['show_id'], 'listing': texplod['listed_in']}
    listings = pd.DataFrame(listings)

    # Drop unnecessary tables from shows

    logging.info('dropping unnecessary columns')
    shows.drop(columns=['director', 'cast', 'listed_in'], inplace=True)

    return (shows, personnel, movie_crew, listings)


def load(shows, personnel, movie_crew, listings):
    engine = create_engine(connection_string)

    logging.info('cleaning up previous runs')
    conn = engine.connect()
    conn.execute('delete from movie_crew')
    conn.execute('delete from listings')
    conn.execute('delete from personnel')
    conn.execute('delete from shows')

    logging.info('loading shows table')
    shows.to_sql(name='shows', con=engine, if_exists='append', index=False)

    logging.info('loading personnel table')
    personnel.to_sql(name='personnel', con=engine, if_exists='append', index=False)
    
    logging.info('loading movie_crew table')
    movie_crew.to_sql(name='movie_crew', con=engine, if_exists='append', index=False)
    
    logging.info('loading listings table')
    listings.to_sql(name='listings', con=engine, if_exists='append', index=False)


def main():
    logging.basicConfig(level=logging.INFO)

    logging.info('extracting csv')
    shows = extract('netflix_titles.csv')

    logging.info('performing transforms')
    shows, personnel, movie_crew, listings = transform(shows)

    logging.info('loading to database')
    load(shows, personnel, movie_crew, listings)

    # engine = create_engine('postgresql://linkfire:linkfire@localhost:5432/netflix')
    # sql_schema = pd.io.sql.get_schema(listings, 'listings', con=engine)
    # print(sql_schema)


main()


