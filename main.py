import requests
from bs4 import BeautifulSoup
import re
import psycopg2
from psycopg2 import sql

UWC_base = 'http://www.mediatraffic.de/tracks-week'

calculation_year = input(str("Enter year of calculation: "))

UWC_list = [UWC_base + f'{week:02}-{calculation_year}.htm' for week in range(1, 53)]

"""
Ввиду особенностей исходной HTML страницы некоторые названия песен записаны в нескольких тегах.
Для этого вводится список SONGS_TO_MERGE для объединения этих названий.
"""

SONGS_TO_MERGE = ['(It Goes Like) Nanana', "I'm Good (Blue)", "Until I Found You",
                  "Celestial", "Special Kiss", "On The Street", "Take Two", "Tapestry",
                  "Nanimono"]
DB_CONNECTION = {"host": "localhost", "database": "song_sales", "user": "postgres",
                 "password": "8848"}


def get_artist_songs(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    art = [element.text.strip().replace('\n', '').replace('\t', '')
           for element in soup.select('b')
           if len(element.text) > 7
           and any(char.isalpha() for char in element.text)]

    artist_songs = []
    merge_next = False
    for i in range(len(art)):
        if merge_next:
            artist_songs.append(art[i - 1] + " " + art[i])
            merge_next = False
        elif art[i] in SONGS_TO_MERGE:
            merge_next = True
        else:
            artist_songs.append(art[i])
    return artist_songs


def get_sales(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    soup = BeautifulSoup(re.sub(r'4408066', '', str(soup)), 'lxml')

    sales_week = []
    for element in soup.select('font'):
        text = element.text.strip().replace('\n', '').replace('\t', '')
        match = re.search(r'[\d.]{5,8}', text)
        if match:
            digits = match.group()
            sales_week.append(digits.replace('.', ''))
    return sales_week


def create_table(cursor, table_name):
    table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} (song TEXT, sales INTEGER)").format(
        sql.Identifier(table_name)
    )
    cursor.execute(table_query)


def save_artists_list_to_database(song_values, sales_values, uwc):
    conn = psycopg2.connect(**DB_CONNECTION)
    cursor = conn.cursor()
    week_number = re.search(r'week(\d+)', uwc)
    year_number = re.search(r'week\d+-(\d+)', uwc)
    if week_number and year_number:
        week = week_number.group(1)
        year = year_number.group(1)
        table_name = f"sales_week_{week}_{year}"
        create_table(cursor, table_name)

        records = [(artist_song, int(sales_week)) for artist_song, sales_week
                   in zip(song_values, sales_values)]

        insert_query = sql.SQL("INSERT INTO {} (song, sales) VALUES (%s, %s)").format(sql.Identifier(table_name))
        cursor.executemany(insert_query, records)

        conn.commit()

        cursor.execute(f"DROP TABLE IF EXISTS sales_week_combined_{year}")
        cursor.execute(
            f"CREATE TABLE sales_week_combined_{year} AS "
            f"SELECT song, SUM(sales) AS sales_sum FROM ("
            + " UNION ALL ".join([f"SELECT song, sales FROM sales_week_{str(w).zfill(2)}_{year}"
                                  for w in range(1, int(week) + 1)]) + ") AS combined GROUP BY song")

        conn.commit()
        cursor.close()
        conn.close()

        print(f"Данные успешно сохранены в базу данных - неделя {week} год {year}")


for uwc_url in UWC_list:
    artist = get_artist_songs(uwc_url)
    sales = get_sales(uwc_url)
    save_artists_list_to_database(artist, sales, uwc_url)

test
