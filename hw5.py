import mysql.connector
import pandas as pd
import json
from mysql.connector import connect, Error

global connection_to_db


def making_connection(host, root, password, database):
    try:
        with connect(
                host=host,
                user=root,
                password=password,
                database=database,
        ) as connection:
            pass
            #print(connection)
            #print("was successfully established")
    except Error as error:
        print(error)
    return connection


def creating_tables(host, root, password, database):
    connection = making_connection(host, root, password, database)


    create_original_table = "CREATE TABLE origin( budget BIGINT, homepage VARCHAR(200), id MEDIUMINT, original_language VARCHAR(5), original_title VARCHAR(100), overview VARCHAR(1000), " \
                            "popularity FLOAT, release_date VARCHAR(20), revenue BIGINT, runtime MEDIUMINT, status SET('Released', 'Rumored', 'Post Production'), tagline VARCHAR(300), title VARCHAR(100), " \
                            "vote_average FLOAT, vote_count INT, PRIMARY KEY(id))"

    create_generes_table = "CREATE TABLE genres( genre_id INT, genre_name TEXT, PRIMARY KEY(genre_id))"

    create_generes_join_table = "CREATE TABLE genres_movies( genre_id INT, id MEDIUMINT, Foreign KEY(genre_id) REFERENCES genres(genre_id), FOREIGN KEY(id)" \
                                "REFERENCES  origin(id), PRIMARY KEY(genre_id, id))"

    create_production_companies_table = "CREATE TABLE pro_companies (company_id INT, company_name TEXT, PRIMARY KEY(company_id))"

    create_production_com_join_table = "CREATE TABLE procomp_movies( company_id INT, id MEDIUMINT, FOREIGN KEY(company_id) REFERENCES pro_companies(company_id), FOREIGN KEY(id)" \
                                       "REFERENCES origin(id), PRIMARY KEY(company_id, id))"

    create_keyword_table = "CREATE TABLE keyword(key_id INT, key_name TEXT, PRIMARY KEY(key_id))"

    create_keyword_movie_join_table = "CREATE TABLE keyword_movies(key_id INT, id MEDIUMINT, FOREIGN KEY(key_id) REFERENCES keyword(key_id), FOREIGN KEY(id)" \
                                      "REFERENCES origin(id), PRIMARY KEY(key_id, id))"

    create_production_countries_table = "CREATE TABLE pro_countries (country_id VARCHAR(10), country_name TEXT, PRIMARY KEY(country_id))"

    create_production_countries_join_table = "CREATE TABLE procountry_movies (country_id VARCHAR(10), id MEDIUMINT, FOREIGN KEY(country_id) REFERENCES pro_countries(country_id)," \
                                             "FOREIGN KEY(id) REFERENCES origin(id), PRIMARY KEY(country_id, id))"

    create_spoken_language_table = "CREATE TABLE spoken_languages (language_id VARCHAR(5), language_name TEXT, PRIMARY KEY (language_id))"

    create_spoken_language_join_table = "CREATE TABLE spoken_movies (language_id VARCHAR(5), id MEDIUMINT, FOREIGN KEY(language_id) REFERENCES spoken_languages(language_id), " \
                                        "FOREIGN KEY(id) REFERENCES origin(id), PRIMARY KEY(language_id, id))"

    connection.reconnect()
    try:
        with connection.cursor() as c:
            c.execute(create_original_table)
            #print("able to create the original table")
            c.execute(create_generes_table)
            #print("able to create genres table")
            c.execute(create_generes_join_table)
            #print("able to create genres_movie join table")
            c.execute(create_production_companies_table)
            #print("able to create pro_company table")
            c.execute(create_production_com_join_table)
            #print("able to create pro_company join table")
            c.execute(create_keyword_table)
            #print("able to create keyword table")
            c.execute(create_keyword_movie_join_table)
            #print("able to create keyword join table")
            c.execute(create_production_countries_table)
            #print("able to create production country table")
            c.execute(create_production_countries_join_table)
            #print("able to create production join table")
            c.execute(create_spoken_language_table)
            #print("able to create spoken language table")
            c.execute(create_spoken_language_join_table)
            #print("able to create spoken language join table")
            connection.commit()

    except Error as error:
        print(error)

    # return(table1)


def parsing_data(host, root, password, database):
    connection = making_connection(host, root, password, database)
    connection.reconnect()
    '''counts number of tuples in total'''
    temp_count = 0
    join_relation_list = []
    id_column_list = []
    '''value_list1 and 2 stores the first and second half of each tupe of the multi_values columns'''
    value_list1 = []
    value_list2 = []

    reader = pd.read_csv(r"C:\Users\zhich\Downloads\tmdb_5000_movies.csv")
    '''columns that compose the original table'''
    reader = reader.where((pd.notnull(reader)), None)
    reader_origin = pd.read_csv(r"C:\Users\zhich\Downloads\tmdb_5000_movies.csv", usecols=['budget', 'homepage', 'id',
                                                                                           'original_language',
                                                                                           'original_title',
                                                                                           'overview',
                                                                                           'popularity',
                                                                                           'release_date',
                                                                                           'revenue',
                                                                                           'runtime',
                                                                                           'status',
                                                                                           'tagline',
                                                                                           'title',
                                                                                           'vote_average',
                                                                                           'vote_count'
                                                                                           ])
    reader_origin = reader_origin.where((pd.notnull(reader)), None)
    '''each tuple in each column is in a list'''
    id_column = reader.id
    budget_column = reader.budget
    homepage_column = reader.homepage
    original_language_column = reader.original_language
    original_title_column = reader.original_title
    overview_column = reader.overview
    popularity_column = reader.popularity
    release_date_column = reader.release_date
    revenue_column = reader.revenue
    runtime_column = reader.runtime
    status_column = reader.status
    tagline_column = reader.tagline
    title_column = reader.title
    vote_average_column = reader.vote_average
    vote_count_column = reader.vote_count

    '''Attributes that are to be separated'''
    genres_column = reader.genres
    keyword_column = reader.keywords
    company_column = reader.production_companies
    country_column = reader.production_countries
    language_column = reader.spoken_languages
    temp_id_column_list = id_column.tolist()
    id_column_index = 0

    '''read in the origin csv file and insert each column into a table that doesn't include the multi_valued attributes'''
    cols = "`,`".join([str(i) for i in reader_origin.columns.tolist()])
    for i, row in reader_origin.iterrows():
        insert_origin_query = "INSERT INTO `origin` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        with connection.cursor() as cursor:
            cursor.execute(insert_origin_query, tuple(row))
            connection.commit()

    for x in genres_column:
        if x == "[]":
            pass
            #print("[]")
        else:

            tuple_dict = json.loads(x)  # change each tuple to a dictionary
            for c in tuple_dict:
                temp_tuple = (c['id'], temp_id_column_list[id_column_index])
                join_relation_list.append(temp_tuple)
            for key in tuple_dict:  # for every row in the attribute
                temp_count += 1
                temp_dict = key  # temp_dict is each tuple in a row
                dict1 = dict(list(temp_dict.items())[:len(temp_dict) // 2])  # split the dictionary to half

                if dict1.values() not in value_list1:
                    value_list1.append(*dict1.values())

                dict2 = dict(list(temp_dict.items())[len(temp_dict) // 2:])  # split the dictionary to half
                if dict2.values() not in value_list2:
                    value_list2.append(*dict2.values())
        id_column_index += 1

    id_column_index = 0
    # print("list1: " + str(value_list1) + "list2: " + str(value_list2))
    list_to_be_inserted = list(tuple(zip(value_list1, value_list2)))
    list_to_be_inserted = list(set(list_to_be_inserted))
    # print(genre_movie_id_list)

    insert_genre_join_query = "INSERT INTO genres_movies (genre_id, id ) VALUES(%s, %s)"
    # insert_genre_id_join_query = "INSERT INTO genres_movies (g_id) VALUES(%s)"
    insert_genre_query = "INSERT INTO genres (genre_id, genre_name) VALUES (%s, %s)"

    with connection.cursor() as cursor:
        cursor.executemany(insert_genre_query, list_to_be_inserted)
        cursor.executemany(insert_genre_join_query, join_relation_list)
        connection.commit()
    value_list1 = []
    value_list2 = []
    join_relation_list = []

    for x in company_column:
        if x == "[]":
            pass
            #print("[]")
        else:
            tuple_dict = json.loads(x)  # change each tuple to a dictionary
            for c in tuple_dict:
                temp_tuple = (c['id'], temp_id_column_list[id_column_index])
                join_relation_list.append(temp_tuple)
            for key in tuple_dict:  # for every row in the attribute
                temp_count += 1
                temp_dict = key  # temp_dict is each tuple in a row
                dict1 = dict(list(temp_dict.items())[:len(temp_dict) // 2])  # split the dictionary to half

                if dict1.values() not in value_list1:
                    value_list1.append(*dict1.values())
                dict2 = dict(list(temp_dict.items())[len(temp_dict) // 2:])  # split the dictionary to half
                if dict2.values() not in value_list2:
                    value_list2.append(*dict2.values())
        id_column_index += 1

    id_column_index = 0
    list_to_be_inserted = list(tuple(zip(value_list2, value_list1)))
    list_to_be_inserted = list(set(list_to_be_inserted))

    insert_pro_comp_join_query = "INSERT INTO procomp_movies (company_id, id ) VALUES(%s, %s)"
    insert_pro_companies_query = "INSERT INTO pro_companies (company_id, company_name) VALUES (%s, %s)"

    with connection.cursor() as cursor:
        cursor.executemany(insert_pro_companies_query, list_to_be_inserted)
        cursor.executemany(insert_pro_comp_join_query, join_relation_list)
        connection.commit()

    value_list1 = []
    value_list2 = []
    join_relation_list = []

    for x in keyword_column:
        if x == "[]":
            pass
            #print("[]")
        else:
            tuple_dict = json.loads(x)  # change each tuple to a dictionary
            for c in tuple_dict:
                temp_tuple = (c['id'], temp_id_column_list[id_column_index])
                join_relation_list.append(temp_tuple)
            for key in tuple_dict:  # for every row in the attribute
                temp_count += 1
                temp_dict = key  # temp_dict is each tuple in a row
                dict1 = dict(list(temp_dict.items())[:len(temp_dict) // 2])  # split the dictionary to half

                if dict1.values() not in value_list1:
                    value_list1.append(*dict1.values())
                dict2 = dict(list(temp_dict.items())[len(temp_dict) // 2:])  # split the dictionary to half
                if dict2.values() not in value_list2:
                    value_list2.append(*dict2.values())
        id_column_index += 1

    id_column_index = 0
    list_to_be_inserted = list(tuple(zip(value_list1, value_list2)))
    list_to_be_inserted = list(set(list_to_be_inserted))
    # print(list_to_be_inserted)
    insert_keyword_join_query = "INSERT INTO keyword_movies (key_id, id ) VALUES(%s, %s)"
    insert_keyword_query = "INSERT INTO keyword (key_id, key_name) VALUES (%s, %s)"

    with connection.cursor() as cursor:
        cursor.executemany(insert_keyword_query, list_to_be_inserted)
        cursor.executemany(insert_keyword_join_query, join_relation_list)
        connection.commit()

    value_list1 = []
    value_list2 = []
    join_relation_list = []

    for x in country_column:
        if x == "[]":
            pass
            #print("[]")
        else:
            tuple_dict = json.loads(x)  # change each tuple to a dictionary
            for c in tuple_dict:
                temp_tuple = (c['iso_3166_1'], temp_id_column_list[id_column_index])
                join_relation_list.append(temp_tuple)
            for key in tuple_dict:  # for every row in the attribute
                temp_count += 1
                temp_dict = key  # temp_dict is each tuple in a row
                dict1 = dict(list(temp_dict.items())[:len(temp_dict) // 2])  # split the dictionary to half

                if dict1.values() not in value_list1:
                    value_list1.append(*dict1.values())
                dict2 = dict(list(temp_dict.items())[len(temp_dict) // 2:])  # split the dictionary to half
                if dict2.values() not in value_list2:
                    value_list2.append(*dict2.values())
        id_column_index += 1

    id_column_index = 0
    list_to_be_inserted = list(tuple(zip(value_list1, value_list2)))
    list_to_be_inserted = list(set(list_to_be_inserted))
    insert_pro_country_join_query = "INSERT INTO procountry_movies (country_id, id ) VALUES(%s, %s)"
    insert_pro_country_query = "INSERT INTO pro_countries (country_id, country_name) VALUES (%s, %s)"

    with connection.cursor() as cursor:
        cursor.executemany(insert_pro_country_query, list_to_be_inserted)
        cursor.executemany(insert_pro_country_join_query, join_relation_list)
        connection.commit()

    value_list1 = []
    value_list2 = []
    join_relation_list = []

    for x in language_column:
        if x == "[]":
            pass
            #print("[]")
        else:

            tuple_dict = json.loads(x)  # change each tuple to a dictionary
            for c in tuple_dict:
                temp_tuple = (c['iso_639_1'], temp_id_column_list[id_column_index])
                join_relation_list.append(temp_tuple)
            for key in tuple_dict:  # for every row in the attribute
                temp_count += 1
                temp_dict = key  # temp_dict is each tuple in a row
                dict1 = dict(list(temp_dict.items())[:len(temp_dict) // 2])  # split the dictionary to half

                if dict1.values() not in value_list1:
                    value_list1.append(*dict1.values())

                dict2 = dict(list(temp_dict.items())[len(temp_dict) // 2:])  # split the dictionary to half
                if dict2.values() not in value_list2:
                    value_list2.append(*dict2.values())
        id_column_index += 1

    id_column_index = 0
    # print("list1: " + str(value_list1) + "list2: " + str(value_list2))
    list_to_be_inserted = list(tuple(zip(value_list1, value_list2)))
    list_to_be_inserted = list(set(list_to_be_inserted))
    # print(genre_movie_id_list)

    insert_sl_movie_join_query = "INSERT INTO spoken_movies (language_id, id ) VALUES(%s, %s)"
    # insert_genre_id_join_query = "INSERT INTO genres_movies (g_id) VALUES(%s)"
    insert_sl_query = "INSERT INTO spoken_languages (language_id, language_name) VALUES (%s, %s)"
    update_null = "update spoken_languages set language_name = NULL where language_name = ''"
    with connection.cursor() as cursor:
        cursor.executemany(insert_sl_query, list_to_be_inserted)
        cursor.executemany(insert_sl_movie_join_query, join_relation_list)
        connection.commit()
    # changes all the empty entries to null
    with connection.cursor() as cursor:
        cursor.execute(update_null)
        connection.commit()
    value_list1 = []
    value_list2 = []
    join_relation_list = []


def queries(query_number, host, root, password, database):
    temp = making_connection(host, root, password, database)
    temp.reconnect()
    cursor = temp.cursor(buffered=True)
    if query_number == 1:
        cursor.execute('SELECT AVG(budget) FROM origin;')
        print('\n')
        result = cursor.fetchall()
        for x in result:
            print(x)
    if query_number == 2:

        cursor.execute(
            "SELECT origin.title AS 'movie_title', pro_companies.company_name AS 'company_name' FROM origin INNER JOIN procomp_movies USING(id) INNER JOIN pro_companies USING(company_id) INNER JOIN procountry_movies USING(id) INNER JOIN pro_countries USING(country_id) WHERE country_id = 'US';")
        print('\n')
        result = cursor.fetchall()
        for x in result:
            print(x)
    if query_number == 3:

        cursor.execute('select title, revenue from origin order by revenue desc limit 5;')
        print('\n')
        result = cursor.fetchall()
        for x in result:
            print(x)

    if query_number == 4:

        cursor.execute(
            'select origin.title, group_concat(genres.genre_name) as genres from genres_movies inner join origin using(id) inner join genres using(genre_id) where origin.id in (select origin.id from genres_movies inner join origin using(id) inner join genres using(genre_id) where genres.genre_name = "Science Fiction" or genres.genre_name = "Mystery" group by origin.title having count(*) = 2) group by origin.title;')
        print('\n')
        result = cursor.fetchall()
        for x in result:
            print(x)

    if query_number == 5:
        cursor.execute(
            'select title, popularity from origin where popularity > (select avg(popularity) from origin) order by popularity DESC;')
        print('\n')
        result = cursor.fetchall()
        for x in result:
            print(x)


if __name__ == "__main__":
    creating_tables("localhost", "root", "zzc5211314", "empty_db")
    # temp_table = creating_tables(temp)
    # creating_tables(temp)
    parsing_data("localhost", "root", "zzc5211314", "empty_db")
    queries(2, "localhost", "root", "zzc5211314", "empty_db")

    # parsing_data(temp)
