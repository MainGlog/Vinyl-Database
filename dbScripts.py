import aiohttp
from os import getenv
from dotenv import load_dotenv
from pyodbc import connect
from time import sleep

load_dotenv()
conn = connect(getenv("SQL_CONNECTION_STRING"))

class DataModel:
    bands: {int, str, str, str}
    artists: {int, str, str, str, str, str, str}
    albums: {int, str, str, int, int}
    records: {int, str, str, str, str, int}
    genres: {int, str, str}

    def __init__(self):
        self.bands = []
        self.artists = []
        self.albums = []
        self.records = []
        self.genres = []

    def select_all(self, table_name):
        query = """
        SELECT *
        FROM """ + table_name

        cursor = conn.cursor()
        cursor.execute(query)

        results = cursor.fetchall()
        
        match table_name:
            case "Bands":
                self.bands = results
            case "Artists":
                self.artists = results
            case "Albums":
                self.albums = results
            case "Records":
                self.records = results
            case "Genres":
                self.genres = results
        


        print(table_name + " contains " + str(len(results)) + " entries")

    def fetch_data(self):
        table_names = [
            "Bands",
            "Artists",
            "Albums",
            "Records",
            "Genres",
        ]
         
        for table in table_names:
            self.select_all(table)

db = DataModel()
db.fetch_data()

def get_id_from_name(table_name, field_name, value):
    query = f"""
    SELECT {table_name}ID
    FROM {table_name}s
    WHERE {field_name} = '{value}';
    """
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

async def get_release_year(album_title, artist):
    token = "cdAtNuyJOYKXpIWZggHbodcQoqorFbElvGGXKYew"
    url = f"https://api.discogs.com/database/search?artist={artist.replace(" ", "+")}&format=Vinyl&release_title={album_title.replace(" ", "+")}&token={token}"

    print(f"Fetching Release Year for {album_title} by {artist}...")

    data = await fetch_from_discogs(url)
    
    try:
        master_url = data["results"][0]["master_url"]
        data = await fetch_from_discogs(master_url)
    except Exception as e:
        print("No Master URL Present")
        return "Error"

    try: 
        year = data["year"]
        if len(str(year)) <= 4:
            year = f"'{year}-01-01'"
        print(f"Release Year: {year}")
        return year
    except Exception as e:
        print("An Error Occurred While Attempting to Access API Data: ", e)
        return "Error"
    


async def fetch_from_discogs(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                print("Error occurred")
                return
            sleep(3)
            return await response.json()


def fetch_latest_album_id():
    cursor = conn.cursor()
    cursor.execute("""SELECT MAX(AlbumId) FROM Albums""")
    return cursor.fetchall()

def insert_record(catNum, artist_type):
    url = "https://api.discogs.com/database/search?catno=" + catNum.replace(" ", "+")
    token = "cdAtNuyJOYKXpIWZggHbodcQoqorFbElvGGXKYew"
    
    url += "&token=" + token

    # Will need to find additional parameter(s) to specify the release, as several can show up for a given cat number 
    record_data = fetch_from_discogs(url)["results"][0]
    pressing_date = record_data["year"]
    pressing_country = record_data["country"]
    
    record_label = record_data["label"]

    master_url = record_data["master_url"]    
    album_data = fetch_from_discogs(master_url)

    print(album_data)


    # Album Table
    title = album_data["title"]
    artist = album_data["artists"][0]
    release_date = album_data["year"]    
    tracks = album_data["tracklist"]

    artistId = 0

    match artist_type:
        case "Band":
            if artist not in db.bands:
                # TODO insert into Bands table
                print('temp')
            else:
                # TODO get BandID
                artistId = db.bands
        case "Artist":
            if artist not in db.artists:
                # TODO insert into Artists table
                print('temp')
            else:
                # TODO get BandID
                artistId = db.artists
        case default:
            print("Invalid Artist Type Selected. Valid Options: Band, Artist")
    
    # Perhaps try to find a way to make the release date fetch the exact date if it's present
    query = """
    INSERT INTO Albums
    VALUES ('""" + title + """', '""" + str(release_date) + """-01-01', 
    """ + "NULL" if artist_type == "Artist" else str(artistId) + """,
    """ + "NULL" if artist_type == "Band" else str(artistId) + """);
    """

    cursor = conn.cursor()
    cursor.execute(query)

    album_id = fetch_latest_album_id()

    query = """
    INSERT INTO Records
    VALUES ('""" + catNum + """', '""" +  str(pressing_date) + """',
     '""" + pressing_country + """', '""" + record_label + """',
     """ + str(album_id) + """)
    """

    styles = album_data[styles]

    for style in styles:
        if style not in db.bands:
            print("Genre not in Database")
            # TODO add it

async def add_existing_albums(file_name):
    file = open(file_name)
    entries: {int, str, str, str, str}


    query = """SELECT * FROM Artists"""
    cursor = conn.cursor()
    cursor.execute(query)
    artists = cursor.fetchall()

    query = """SELECT * FROM Bands"""
    cursor = conn.cursor()
    cursor.execute(query)
    bands = cursor.fetchall()

    final_insert_query = """
    INSERT INTO Albums VALUES
    """
    
    id = 1
    for line in file:
        is_artist = True

        updated_line = line.strip("('").replace("),", "")
        segments = updated_line.split("',")
        
        artist = segments[0].strip("'")
        title = segments[1].strip("'").replace("\\\\", "'")
        release_year = await get_release_year(title.replace('\'\'', '\''), artist)

        if release_year == 'Error':
            print(f"An Error Occurred on {title} by {artist}")
            continue

        #print(f"Artist: {artist}")
        #print(f"Title: {title}")
        #print(f"Release Year: {release_year}")
        
        
        # Determines if the artist is a band or not 
        for band in bands:
            current_band = bands

            if artist in band[1]:
                is_artist = False
                break


        if is_artist:
            table = "Artist" if is_artist else "Band"
            field_to_check = "FirstName" if is_artist else "BandName"     
        
            query = f"""
            SELECT {table}ID
            FROM {table}s
            WHERE {field_to_check} = '{artist.split(" ", 1)[0] if is_artist else artist}'
            {f"AND LastName = '{artist.split(" ", 1)[1]}'" if is_artist else ""}
            """
            
            cursor.execute(query)
            artist_id = cursor.fetchall()        
        else:
            artist_id = get_id_from_name("Band", "BandName", artist)


        # print(artist_id)

        # print(f"{id}: {artist}, {title}, {release_year}, {artist if is_artist else "NULL"}, {artist if is_artist == False else "NULL"}")
        final_insert_query += f"""
        ('{title}', '{release_year}', {artist_id if is_artist else "NULL"}, {artist_id if is_artist == False else "NULL"}),
        """

        id += 1
        # entries.update(id, )
    final_insert_query = final_insert_query.removesuffix(",")
    final_insert_query += ";"
    print(final_insert_query)

file_name = "temp.txt"

async def main():
    await add_existing_albums(file_name)

import asyncio
asyncio.run(main())

# insert_record("BS 2695", "Band")
