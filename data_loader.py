import json
import urllib
import time

def get_top_1000():
    results = json.load(urllib.urlopen("https://www.kimonolabs.com/api/4ymbn5k6?&apikey=PKc2uIP4kwg7HKxQKWgDGlvyFg4VHJw8&kimmodify=1"))
    return results

def get_movie_data(id):
    result = json.load(urllib.urlopen("http://www.omdbapi.com/?i="+id+"&plot=full&r=json"))
    return result

def main():
    corpus = []
    top_movies = get_top_1000()
    i = 0
    for movie in top_movies:
        movie_data = get_movie_data(movie['id'])
        print str(i) + ": " + movie_data['Title']
        corpus.append(movie_data)
        i = i + 1
        time.sleep(.25)

    print corpus

main()