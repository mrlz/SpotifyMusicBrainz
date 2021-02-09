import requests
import xmltodict
import csv
import time
from multiprocessing import Pool
import numpy as np
import random
stopwords = ['&']
########## Artist info from artist table#########
def get_artist_info(artist):
    return_values = []
    name = ""
    if 'name' in artist:
        name = artist['name']
    country = ""
    if 'country' in artist:
        country = artist['country']
    arid = ""
    if '@id' in artist:
        arid = artist['@id']
    begin_area = ""
    if 'begin-area' in artist:
        if 'name' in artist['begin-area']:
            begin_area = artist['begin-area']['name']
    return_values.append(name)
    return_values.append(country)
    return_values.append(arid)
    return_values.append(begin_area)
    return return_values

def get_artist_list_info(artist_list):
    return_values = []
    if 'artist' in artist_list:
        artist_list = artist_list['artist']
        if (isinstance(artist_list, list)):
            for artist in artist_list:
                return_values.append(get_artist_info(artist))
        else:
            return_values.append(get_artist_info(artist_list))
    return return_values

def get_artist_candidates(artist_name):
    lock = True
    while(lock):
        r = requests.get('http://musicbrainz.org/ws/2/artist/?query=artist:' + artist_name)
        if(r.status_code == 200):
            lock = False
        if(r.status_code != 200):
            lock = True
            print("looping: " + artist_name + ":", time.time())
            time.sleep(2 + random.randint(1,4))
            print("out of sleep:,",time.time())
    # print(r.status_code)
    my_dict = xmltodict.parse(r.text)
    artist_list = my_dict['metadata']['artist-list']
    return get_artist_list_info(artist_list)
################################################

########## Artist info from work table##########
def get_artist_info_at_work_list(artist, name):
    return_values = []
    artist_name = ""
    if 'name' in artist:
        artist_name = artist['name']
    arid = ""
    if '@id' in artist:
        arid = artist['@id']
    return_values.append(artist_name)
    return_values.append(arid)
    return_values.append(name)
    return return_values

def get_artist_info_at_work(artist, name):
    return_values = []
    if (isinstance(artist, list)):
        for a in artist:
            return_values.append(get_artist_info_at_work_list(a, name))
    else:
        return_values.append(get_artist_info_at_work_list(artist, name))
    return return_values

def get_relation_info(relation, name):
    return_values = []
    if (isinstance(relation, list)):
        for artist in relation:
            if 'artist' in artist:
                return_values.append(get_artist_info_at_work(artist['artist'], name))
    else:
        if 'artist' in relation:
            return_values.append(get_artist_info_at_work(relation['artist'], name))
    return return_values

def get_relation_list_info(relation_list, name):
    return_values = []
    if(isinstance(relation_list, list)):
        for relation in relation_list:
            if(relation['@target-type']  == 'artist'):
                return_values.append(get_relation_info(relation['relation'], name))
    else:
        if(relation_list['@target-type']  == 'artist'):
            return_values.append(get_relation_info(relation_list['relation'], name))
    return return_values

def get_work_list_info(work_list):
    return_values = []
    if 'work' in work_list:
        work_list = work_list['work']
        if (isinstance(work_list, list)):
            for work in work_list:
                name = ""
                if 'title' in work:
                    name = work['title']
                if 'relation-list' in work:
                    return_values.append(get_relation_list_info(work['relation-list'], name))
        else:
            if 'relation-list' in work_list:
                name = ""
                if 'title' in work_list:
                    name = work_list['title']
                return_values.append(get_relation_list_info(work_list['relation-list'], name))
    return return_values

#We can obtain an unrolled version with a little bit of extra coding in the
#above functions. So there you have it, future work!
def get_work_list_info_unrolled(work_list):
    rolled_info = get_work_list_info(work_list)
    return_values = []
    for work in rolled_info:
        for relation_list in work:
            for relation in relation_list:
                for artist in relation:
                    return_values.append(artist)
    return return_values

def get_song_candidates(song_name):
    lock = True
    while(lock):
        r2 = requests.get('https://beta.musicbrainz.org/ws/2/work/?query=work:' + song_name)
        if(r2.status_code == 200):
            lock = False
        if(r2.status_code != 200):
            lock = True
            print("looping: " + song_name + ":", time.time())
            time.sleep(2 + random.randint(1,4))
            print("out of sleep:,",time.time())

    my_dict2 = xmltodict.parse(r2.text)
    work_list = my_dict2['metadata']['work-list']
    return get_work_list_info_unrolled(work_list)
###################################################

############# Matches artists with works and ######
############# returns the most relevant group #####
############# using the difference in characters ##
############# as criteria #########################
def get_related_works(artist, artist_work_pairs):
    related = []
    for awp in artist_work_pairs:
        if(artist[2] == awp[1]):
            related.append(artist + [awp[2]])
    return related

def get_most_relevant_tuple(artist_name, song_name):
    artist_name_original = artist_name
    artist_name = artist_name.lower().replace('&', '').replace("  ", " ")
    # print(artist_name)
    song_name = song_name.lower()
    # print(song_name)

    #We get a list that contains tuples (artist name, country, artist_id, begin_location)
    artist_candidates = get_artist_candidates(artist_name)
    # print("Artists")
    # for a in artist_candidates:
    #     print(a)
    #We get a list that contains tuples (artist name, artist_id, song_name)
    song_candidates = get_song_candidates(song_name)
    # print("Songs")
    # for b in song_candidates:
    #     print(b)

    final_candidates = []
    just_artist = []
    for artist in artist_candidates:
        related = get_related_works(artist, song_candidates)
        artist_name_lower = artist[0].lower()
        if ((artist_name in artist_name_lower) or (artist_name_original in artist_name_lower)):
            copy = artist
            copy.append("")
            copy.append(abs( len(artist_name_lower) - len(artist_name_original) ))
            just_artist.append(copy)
        for work in related:
            related_artist_name = work[0].lower()
            related_song_name = work[4].lower()
            if ((artist_name in related_artist_name) and (song_name in related_song_name)):
                work.append(abs(len(related_artist_name)-len(artist_name)) + abs(len(related_song_name)-len(song_name)))
                final_candidates.append(work)

    if final_candidates:
        final_candidates.sort(key=lambda x: x[5], reverse=False)
        return final_candidates[0]
    elif just_artist:
        just_artist.sort(key=lambda x: x[5], reverse=False)
        return just_artist[0]
    else:
        return final_candidates

###################################################

########### We can finally generate the############
## approximate (artist, country, starting place) ##
################### tuples ########################

def get_artist_country_starting_place(row):
    artist_name = row[0]
    song_name = row[1]
    most_relevant_tuple = get_most_relevant_tuple(artist_name, song_name)
    if most_relevant_tuple:
        return [artist_name, song_name, most_relevant_tuple[0], most_relevant_tuple[1], most_relevant_tuple[3]]
    else:
        return [artist_name, song_name, "", "", ""]
###################################################


pool = Pool(processes = 3)
#Needs an artist_song.csv file which contains all the
#unique (artist, song) tuples from the spotify
#dataset, which are 19586 entries.
with open('artist_song.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    pairs = list(reader)
    nume = int(len(pairs)/2)

    #Divide in two sets to maximize probability of
    #correctly writting to disk at least a part.
    dataset = pairs[1:nume]

    start = time.time()
    results = pool.map(get_artist_country_starting_place, dataset)
    end = time.time()
    print("Requesting took: ", end-start)
    with open('artist_country.csv', 'w') as csvfile2:
        fieldnames = ['artist', 'song', 'result_artist', 'country', 'starting_place']
        writer = csv.DictWriter(csvfile2, fieldnames = fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow({'artist': row[0].replace(',', ''), 'song': row[1].replace(',', ''), 'result_artist': row[2].replace(',', ''), 'country': row[3].replace(',', ''), 'starting_place': row[4].replace(',', '')})

    dataset = pairs[nume:len(pairs)]
    start = time.time()
    results = pool.map(get_artist_country_starting_place, dataset)
    end = time.time()
    print("Requesting took: ", end-start)
    with open('artist_country2.csv', 'w') as csvfile2:
        fieldnames = ['artist', 'song', 'result_artist', 'country', 'starting_place']
        writer = csv.DictWriter(csvfile2, fieldnames = fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow({'artist': row[0].replace(',', ''), 'song': row[1].replace(',', ''), 'result_artist': row[2].replace(',', ''), 'country': row[3].replace(',', ''), 'starting_place': row[4].replace(',', '')})
