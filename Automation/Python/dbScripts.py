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
        


        # print(table_name + " contains " + str(len(results)) + " entries")

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
    return cursor.fetchone()


async def fetch_from_discogs(url):
    async with aiohttp.ClientSession() as session:
        try:
            async with await session.get(url) as response:
                if response.status != 200:
                    print(f"Got status code {response.status} from {url}")
                    await asyncio.sleep(2.5)  # Use asyncio.sleep instead of time.sleep in async functions
                    return None
                # 2 second request limit
                await asyncio.sleep(2.5)  
                return await response.json()
        except aiohttp.ClientError as e:
            print(f"Network error while fetching {url}: {e}")
            return None


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


async def get_release_year(album_title, artist):
    compilations = [
        "Pure Gold"
    ]

    token = "cdAtNuyJOYKXpIWZggHbodcQoqorFbElvGGXKYew"
    url = f"https://api.discogs.com/database/search?artist={artist.replace(" ", "+")}&format=Vinyl{"+Album" if album_title not in compilations else ""}&release_title={album_title.replace(" ", "+")}&token={token}"

    print("\n--------------------------------------------------------------------------------------")
    print(f"Fetching Release Year for {album_title} by {artist}...")

    data = await fetch_from_discogs(url)
    
    if data and "results" in data:
        results = data["results"]
        
        data_found = False
        master_urls = []

        for result in results:
            # Checks that there is a master url
            if result["master_url"] != "null" and str(album_title).upper() in str(result["title"]).upper():
                master_url = result["master_url"]
                
                # Prevents checking the same master url twice for a given album
                if master_url in master_urls:
                    continue
                
                print("\nOriginal URL: ", url)
                print("Master URL: ", master_url)
                
                master_urls.append(master_url)
                master_data = None

                try:
                    master_data = await fetch_from_discogs(master_url)
                    
                    if master_data is not None:
                        data_found = True
                        break
                    else:
                        print("Data Not Found, Trying Next Master URL...")
                        await asyncio.sleep(1)  # Add a small delay before trying next URL
                except Exception as e:
                    print(f"Error fetching master URL {master_url}: {e}")
                    await asyncio.sleep(1)  # Add a small delay before trying next URL
                
                    
        if data_found == False:
            return "Error"
        
    try: 
        year = master_data["year"]
        print(f"\nRelease Year: {year}")
        if len(str(year)) <= 4:
            year = f"'{year}-01-01'"
        return year
    except Exception as e:
        print("\nAn Error Occurred While Attempting to Access API Data: ", e)
        return "Error"

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

    final_insert_query = """INSERT INTO Albums VALUES"""
    
    id = 1
    for line in file:
        #if line.startswith("('Black S") == False:
        #    continue
        is_artist = True

        updated_line = line.strip("('").replace("),", "")
        segments = updated_line.split("',")
        
        artist = segments[0].strip("'")
        title = segments[1].strip("'").replace("\\\\", "'")
        release_year = await get_release_year(title.replace('\'\'', '\''), artist)

        if release_year == 'Error':
            print(f"\nAn Error Occurred on {title} by {artist}")
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
            artist_id = cursor.fetchone()   
        else:
            artist_id = get_id_from_name("Band", "BandName", artist)[0]

        # print(artist_id)
        print("--------------------------------------------------------------------------------------")

        # print(f"{id}: {artist}, {title}, {release_year}, {artist if is_artist else "NULL"}, {artist if is_artist == False else "NULL"}")
        final_insert_query += f"""\n('{title}', {release_year}, {artist_id if is_artist else "NULL"}, {artist_id if is_artist == False else "NULL"}),"""

        id += 1
        # entries.update(id, )
    final_insert_query = final_insert_query.removesuffix(",")
    final_insert_query += ";"
    print("\nFinal Insert Query")
    print(final_insert_query)

async def add_songs(file_name):
    file = open(file_name)

file_name = "DataFromOriginalDB/AlbumsAndArtists.txt"

async def main():
    await add_existing_albums(file_name)

import asyncio
asyncio.run(main())

# insert_record("BS 2695", "Band")
