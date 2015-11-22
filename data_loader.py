import json
import urllib
import time
import os
import psycopg2
import urlparse
import socket

actor_count = 1866
director_count = 509

def get_top_1000():
    results = json.load(urllib.urlopen("https://www.kimonolabs.com/api/4ymbn5k6?&apikey=PKc2uIP4kwg7HKxQKWgDGlvyFg4VHJw8&kimmodify=1"))
    return results

def get_movie_data_by_id(id):
    try:
        result = json.load(urllib.urlopen("http://www.omdbapi.com/?i="+id+"&plot=full&r=json"))
    except Exception as e:
        global actor_count
        global director_count
        print "actor count " + str(actor_count)
        print "director_count " + str(director_count)
        raise

    return result

def main(start):
    top_movies = get_top_1000()[start:]

    # reset_tables()

    for i,movie in enumerate(top_movies):
        movie_data = get_movie_data_by_id(movie['id'])

        print str(i) + ": " + movie_data['Title']

        actors = [x.strip() for x in movie_data['Actors'].split(',')]
        directors = [x.strip() for x in movie_data['Director'].split(',')]
        insert_movie(movie_data,actors,directors)
        # time.sleep()

def reset_tables():
    urlparse.uses_netloc.append("postgres")
    url = urlparse.urlparse("postgres://cgzhugtkhkkipy:hImoFvY1K86iSY5rCcqjK40Yyt@ec2-54-204-20-164.compute-1.amazonaws.com:5432/d4q87lrmqau28e")

    con = psycopg2.connect(
      database=url.path[1:],
      user=url.username,
      password=url.password,
      host=url.hostname,
      port=url.port
    )

    with con:
        cur = con.cursor()
        cur.execute("DROP TABLE actors")
        cur.execute("DROP TABLE directors")
        cur.execute("DROP TABLE movies")

        cur.execute("""CREATE TABLE movies(
            imdb_id TEXT PRIMARY KEY,
            title TEXT,
            year INTEGER,
            rating TEXT,
            released TEXT,
            runtime TEXT,
            genre TEXT,
            plot TEXT,
            language TEXT,
            country TEXT,
            awards TEXT,
            poster TEXT,
            metascore REAL
        );""")

        cur.execute("""CREATE TABLE actors(
            row_id INTEGER PRIMARY KEY,
            imdb_id TEXT,
            name TEXT
        );""")

        cur.execute("""CREATE TABLE directors(
            row_id INTEGER PRIMARY KEY,
            imdb_id TEXT,
            name TEXT
        );""")



def insert_movie(movie_data,actors,directors):
    urlparse.uses_netloc.append("postgres")
    url = urlparse.urlparse("postgres://cgzhugtkhkkipy:hImoFvY1K86iSY5rCcqjK40Yyt@ec2-54-204-20-164.compute-1.amazonaws.com:5432/d4q87lrmqau28e")

    con = psycopg2.connect(
      database=url.path[1:],
      user=url.username,
      password=url.password,
      host=url.hostname,
      port=url.port
    )

    with con:
        cur = con.cursor()

        cur.execute("SELECT imdb_id FROM movies WHERE imdb_id = \'" + movie_data['imdbID']+ "\'")
        if cur.fetchone():
            return

        if movie_data['Metascore'] == 'N/A':
            movie_data['Metascore']= -1

        movie_row = (
            movie_data['imdbID'].encode("utf-8"),
            movie_data['Title'].encode("utf-8"),
            int(movie_data['Year']),
            movie_data['Rated'].encode("utf-8"),
            movie_data['Released'].encode("utf-8"),
            movie_data['Runtime'].encode("utf-8"),
            movie_data['Genre'].encode("utf-8"),
            movie_data['Plot'].encode("utf-8"),
            movie_data['Language'].encode("utf-8"),
            movie_data['Country'].encode("utf-8"),
            movie_data['Awards'].encode("utf-8"),
            movie_data['Poster'].encode("utf-8"),
            float(movie_data['Metascore']))
        
        cur.execute("""INSERT INTO movies VALUES(%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s)""",movie_row)

        for actor in actors:
            global actor_count
            actor_row = [actor_count,movie_data['imdbID'],actor]
            cur.execute("INSERT INTO actors VALUES(%s,%s,%s);", actor_row)
            actor_count = actor_count + 1

        for director in directors:
            global director_count
            director_row = [director_count,movie_data['imdbID'],director]
            cur.execute("INSERT INTO directors VALUES(%s,%s,%s);", director_row)
            director_count = director_count + 1

        con.commit()

main(467)