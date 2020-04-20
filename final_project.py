
import requests
import json
from bs4 import BeautifulSoup
import sqlite3
from flask import Flask, render_template, request
import plotly.graph_objects as go

# GLOBAL CONSTANTS

BASE_URL = 'https://data.worldbank.org'

CACHE_FILENAME = 'fp_cache.json'

DB_FILENAME = 'FinalProject.sqlite'

#   PREPARE THE LIST OF COUNTRIES

def get_country_url():
    """
    Execute once at the beginning of the program.
    :return: countries' name with their urls (dict)
    """

    soup = make_soup_with_cache(BASE_URL+'/country')
    country_list_1 = soup.find_all('section', class_="nav-item")
    country_list_2 = []
    for cl1 in country_list_1:
        country_list_2.extend(cl1.find_all('a'))
    country_url = {}
    for cl2 in country_list_2:
        country_url[cl2.text] = cl2['href']

    return country_url



### GETTING DATA

def get_data(country_name, country_url):
    profile_url = get_country_profile_url(country_name, country_url)
    data_dict = get_country_data(profile_url)

    return data_dict


##  SCRAPING

def get_country_profile_url(country_name, country_url):
    """
    From the country selected, get the profile url.
    :param country_name: a country's name (str)
    :param country_url: from function get_countries (dict)
    :return: the url of the country's profile
    """
    url = BASE_URL + country_url[country_name]
    soup = make_soup_with_cache(url)
    profile_url = soup.find('a', class_="links btn-item icon-flag")['href']

    return profile_url

def get_country_data(profile_url):
    """
    Return some of the data in the profile.
    :param profile_url: from function get_country_profile_url (str)
    :return: mixed data, i.e. all data in the table (list)
    """
    soup = make_soup_with_cache(profile_url)
    mixed_data = []
    for tr in soup.find_all('tr'):
        td_list = []
        for td in tr.find_all('td'):
            td_list.append(td.text)
        mixed_data.append(td_list)
    table_names = get_table_names(soup)
    data_dict = process_mixed_data(mixed_data, table_names)

    # Adding time list
    time_list = ['Year']
    time_list.extend(mixed_data[2][1:])
    for key, val in data_dict.items():
        data_dict[key].append(time_list)

    return data_dict

#   GET TABLE NAMES

def get_table_names(soup):

    table_names = []
    for tr in soup.find_all('tr', class_="custom-row"):
        table_names.append(tr.text)
    # get rid of the title and notes
    table_names = table_names[1:]
    table_names = table_names[:-3]

    return table_names

#   MIXED DATA PROCESSING

def process_mixed_data(mixed_data, table_names):
    # The values in the dict is a list of lists.
    mark = []
    for i in range(len(mixed_data)):
        if mixed_data[i][0] in table_names or mixed_data[i][0] == ' ':
            mark.append(i)
    data_dict = {}
    for j in range(len(table_names)):
        data_dict[table_names[j]] = mixed_data[mark[j]+1:mark[j+1]]

    return data_dict

#   CACHING FUNCTIONS

def open_cache():

    try:
        cache_file = open(CACHE_FILENAME, 'r')
        cache_contents = cache_file.read()
        cache_dict = json.loads(cache_contents)
        cache_file.close()
    except:
        cache_dict = {}

    return cache_dict

def save_cache(cache_dict):

    dumped_json_cache = json.dumps(cache_dict)
    cache_file = open(CACHE_FILENAME, "w")
    cache_file.write(dumped_json_cache)
    cache_file.close()

def make_soup_with_cache(url):
    # It is the text response that saved into cache.
    cache_dict = open_cache()
    if url in cache_dict:
        print("Using Cache")
        response = cache_dict[url]
        soup = BeautifulSoup(response, 'html.parser')
    else:
        print("Fetching")
        response = requests.get(url).text
        cache_dict[url] = response
        save_cache(cache_dict)
        soup = BeautifulSoup(response, 'html.parser')

    return soup


##   CREATE DATABASE

def create_db(data_dict):

    conn = sqlite3.connect(DB_FILENAME)
    cur = conn.cursor()

    sq_data_dict = make_sq_data_dict(data_dict)

    # Construct the queries
    drop_tables_queries = []
    create_tables_queries = []
    for key, val in sq_data_dict.items():
        drop_tables_queries.append(f'DROP TABLE IF EXISTS "{key}"')
        field_names = []
        # for each field in the data list of lists
        for field in val:
            # select the first item, i.e. field name
            field_names.append(field[0])
        query = f'CREATE TABLE IF NOT EXISTS "{key}" (' \
                f'"#" INTEGER PRIMARY KEY AUTOINCREMENT, ' \
                f'"Country" TEXT NOT NULL'
        # iterate through field names to construct the query
        for fn in field_names:
            query += f', "{fn}" REAL'
        query += ')'
        create_tables_queries.append(query)

    # Execute.
    for dtq in drop_tables_queries:
        cur.execute(dtq)
    for ctq in create_tables_queries:
        cur.execute(ctq)

    conn.commit()
    conn.close()

#   PROCESSING TABLE NAMES AND FIELD NAMES

def make_sq_data_dict(data_dict):

    # Get the proper table names.
    sq_data_dict = {}
    for key, val in data_dict.items():
        # also change the keys
        sq_data_dict[process_table_names(key)] = val

    # Get the proper field names.
    sq_data_dict = process_field_name(sq_data_dict)

    return sq_data_dict

def process_table_names(table_name):

    sq_table_name = ''
    for i in range(len(table_name)):
        if table_name[i-1] == ' ':
            sq_table_name += table_name[i].upper()
        elif table_name[i] != ' ':
            sq_table_name += table_name[i]

    return sq_table_name

def process_field_name(sq_data_dict):

    for key, val in sq_data_dict.items():
        # for the index of every field
        for i in range(len(val)):
            field_name = ''
            # for the letters in the name of the field
            for v in val[i][0]:
                if v != ' ':
                    field_name += v
            sq_data_dict[key][i][0] = field_name

    return sq_data_dict


## SAVING

def save_data(country_name, data_dict):

    conn = sqlite3.connect(DB_FILENAME)
    cur = conn.cursor()

    sq_data_dict = make_sq_data_dict(data_dict)
    sq_data_dict = process_strange_values(sq_data_dict)

    print(sq_data_dict)

    save_queries = []
    for key, val in sq_data_dict.items():
        # save different years separately
        for i in range(len(val[0])):
            query = f'INSERT INTO {key} VALUES (NULL, "{country_name}"'
            # exclude the index of field names
            if i != 0:
                for field in val:
                    if field[0] == 'Year':
                        query += f', {int(field[i])}'
                    elif field[i] != 'NULL':
                        query += f', {float(field[i])}'
                    else:
                        query += f", NULL"
                query += ')'
                print(query)
                save_queries.append(query)

    for sq in save_queries:
        cur.execute(sq)

    conn.commit()
    conn.close()

#   DEAL WITH STRANGE VALUES

def process_strange_values(sq_data_dict):

    for key, val in sq_data_dict.items():
        # for the index of the fields
        for i in range(len(val)):
            # for each value in a field
            for j in range(len(val[i])):
                if j != 0:
                    if val[i][j] == '..':
                        sq_data_dict[key][i][j] = 'NULL'
                    if ',' in val[i][j]:
                        no_comma = ''
                        for k in val[i][j]:
                            if k != ',':
                                no_comma += k
                        val[i][j] = no_comma


    return sq_data_dict



### PRESENTATION


##  WEB PAGE SETUP


##  PLOTTING



### MAIN ENTRANCE

def main():

    # Create database
    country_url = get_country_url()
    country_sample = 'Afghanistan'
    data_dict = get_data(country_sample, country_url)
    create_db(data_dict)

    # Get the data of all the countries
    for country_name in country_url:
        try:
            data_dict = get_data(country_name, country_url)
            save_data(country_name, data_dict)
        except TypeError:
            continue
        if country_name == 'Canada':
            break

    # Construct the web page

if __name__ == '__main__':

    main()