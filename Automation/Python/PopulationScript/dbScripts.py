import asyncio
import aiohttp
import json
import requests
from os import getenv, getcwd
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
        query: str = """
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

def get_id_from_name(table_name: str, field_name: str, value: str):
    query = f"""
    SELECT {table_name}ID
    FROM {table_name}s
    WHERE {field_name} = '{value}';
    """
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchone()


async def fetch_from_url(url: str, timeout: float, headers = {}):
    async with aiohttp.ClientSession() as session:
        try:
            async with await session.get(url, headers=headers) as response:
                if response.status != 200:
                    print(f"Got status code {response.status} from {url}")
                    await asyncio.sleep(timeout)  # Use asyncio.sleep instead of time.sleep in async functions
                    return None
                # 2 second request limit
                await asyncio.sleep(timeout)  
                return await response.json()
        except aiohttp.ClientError as e:
            print(f"Network error while fetching {url}: {e}")
            return None


def fetch_latest_album_id():
    cursor = conn.cursor()
    cursor.execute("""SELECT MAX(AlbumId) FROM Albums""")
    return cursor.fetchall()

async def insert_record(catNum: str, artist_type: str):
    url = "https://api.discogs.com/database/search?catno=" + catNum.replace(" ", "+")
    token = "cdAtNuyJOYKXpIWZggHbodcQoqorFbElvGGXKYew"
    
    url += "&token=" + token

    # Will need to find additional parameter(s) to specify the release, as several can show up for a given cat number 
    record_data = await fetch_from_url(url, 2.5)
    result = record_data["results"][0]
    pressing_date = result["year"]
    pressing_country = result["country"]
    
    record_label = result["label"][0]

    master_url = result["master_url"]    
    album_data = await fetch_from_url(master_url, 2.5)

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

    # cursor = conn.cursor()
    # cursor.execute(query)
    print(query)
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

async def fetch_master_data(album_title, artist):
    compilations = [
        "Pure Gold"
    ]

    token = "cdAtNuyJOYKXpIWZggHbodcQoqorFbElvGGXKYew"
    url = f"https://api.discogs.com/database/search?artist={artist.replace(" ", "+")}&format=Vinyl{"+Album" if album_title not in compilations else ""}&release_title={album_title.replace(" ", "+")}&token={token}"

    print("\n--------------------------------------------------------------------------------------")

    data = await fetch_from_url(url, 2.5)
    
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
                    master_data = await fetch_from_url(master_url, 2.5)
                    
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
        else:
            return await fetch_from_url(master_data["main_release_url"], 2.5)      

async def get_release_year(album_title, artist):
    master_data = await fetch_master_data(album_title, artist)

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

    query = """SELECT * FROM Artists"""
    cursor = conn.cursor()
    cursor.execute(query)

    query = """SELECT * FROM Bands"""
    cursor = conn.cursor()
    cursor.execute(query)
    bands = cursor.fetchall()

    final_insert_query = """INSERT INTO Albums VALUES"""
    
    id = 1
    for line in file:
        # Debugging line 
        #if line.startswith("('Black S") == False:
        #    continue
        is_artist = True

        updated_line = line.strip("('").replace("),", "")
        segments = updated_line.split("',")
        
        artist = segments[0].strip("'")
        title = segments[1].strip("'").replace("\\\\", "'")

        master_data = fetch_master_data(title, artist)

        try: 
            release_year = master_data["year"]
            print(f"\nRelease Year: {year}")
            if len(str(year)) <= 4:
                year = f"'{year}-01-01'"
        except Exception as e:
            print(f"\nAn Error Occurred on {title} by {artist}", e)
            continue

        add_songs(id, master_data["tracklist"])


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

async def add_songs(album_id, tracklist):
    for track in tracklist:
        

    # Get the master url for the release
    # Extract the songs into a collection
    # Add a new song entry for each track
    #   Title, Length, AlbumID, TrackPosition
    # Add associated credits for each song
    #   IsWritingCredit(might remove), Part, ArtistID, SongID




        title = track["title"]
        length = track["duration"]
        track_position = track["position"]
        song_insert_query = "INSERT INTO Songs VALUES"

        song_insert_query += f"\n('{title}', '{length}', {str(album_id)}', '{track_position}');"

        cursor = conn.cursor()
        cursor.execute(song_insert_query)

        cursor.execute("SELECT MAX(SongID) FROM Songs;")
        song_id = cursor.fetchall()

        for credit in track["extraartists"]:
            parts = credit["role"].split(",")
            artist = credit["name"]

            artist_id = get_artist_id_from_name(artist)
    
            if artist_id == -1:
                artist_id = add_artist(credit["resource_url"])

            for part in parts:
                credit_insert_query = "INSERT INTO Credits VALUES"
                credit_insert_query += f"\n('{part}', {str(artist_id)}, {str(song_id)}),"
            

file_name = "DataFromOriginalDB/AlbumsAndArtists.txt"

async def get_artist_id_from_name(artist_name):
    names = artist_name.split(" ")
    first_name = names[0]
    last_name = names[1]

    query = f"""
    SELECT ArtistID
    FROM Artists
    WHERE (FirstName = {first_name} AND LastName = {last_name})
    OR (StageFirstName = {first_name} AND StageLastName = {last_name});
    """

    cursor = conn.cursor()
    cursor.execute(query)

    id = cursor.fetchall()
    
    if len(id) == 0:
        return -1
    else:
        return id[0]

# Adds an artist to the DB based on a Discogs URL, then returns the ID of the artist added
async def add_artist(url) -> int:
    artist = await fetch_from_url(url, 2.5)
    
    name = artist["name"].split(" ")
    first = name[0]
    middle = ""
    last = ""
    match len(name):
        case 2:
            last = name[1]
        case 3:
            middle = name[1]
            last = name[2]

    stage_name_present = True
    
    # TODO figure out how to split up the profile at the date. I'm thinking Regular Expressions
    birth_date = artist["profile"]
    
    wiki_url = "https://api.wikimedia.org/core/v1/wikipedia/en/search/page/"
    search_query = f"{first}{f" {middle}" if len(middle) > 0 else ""}{f" {last}" if len(last) > 0 else ""}"
    parameters = {'q': search_query, 'limit': 1}

    with open("Python/PopulationScript/Wikipedia.json") as file:
        credentials = json.load(file)
    access_token = credentials["Token"]

    headers = {
        'Authorization': f'Bearer {access_token}',
        'User-Agent': 'Vinyl DB'
    }

    print(wiki_url)
    print(search_query)
    print(headers)
    async with aiohttp.ClientSession() as session:
            try:
                async with await session.get(wiki_url, headers=headers, params=parameters) as response:
                    if response.status != 200:
                        print(f"Got status code {response.status} from {wiki_url}")
                        await asyncio.sleep(3)  # Use asyncio.sleep instead of time.sleep in async functions
                        return None
                    # 2 second request limit
                    await asyncio.sleep(3)  
                    return await response.json()
            except aiohttp.ClientError as e:
                print(f"Network error while fetching {wiki_url}: {e}")
                return None

    print(response)
    return

    for url in urls:
        if "wikipedia" in url:
            wiki_url = url.replace("//en.", "//api.")
            

            #results = await fetch_from_url(wiki_url, 3, headers)
            #response = requests.get(wiki_url, headers=headers)
            #print(response)
            break
            




    try: 
        real_name = artist["realname"].split(" ")
        first_name = real_name[0]
        match len(real_name):
            case 2:
                last_name = real_name[1]
            case 3:
                middle_name = real_name[1]
                last_name = real_name[2]
    except Exception as e:
        print("No stage name found")
        stage_name_present = False
    
    query = f"""
    INSERT INTO Artists VALUES
    ('{first_name if stage_name_present else first}', '{middle_name if stage_name_present else middle}', '{last_name if stage_name_present else last}',
    '{first if stage_name_present == False else 'NULL'}, {last if stage_name_present == False else 'NULL'}, '{birth_date}', '{middle if stage_name_present == False else 'NULL'}');
    SELECT MAX(ArtistID)
    FROM Artists;
    """
    #cursor = conn.cursor()
    #cursor.execute(query)

    # TODO add memberships
 
    #artist_id = cursor.fetchall()[0] 
    #return artist_id

    
async def main():
    #await insert_record("BS 2695", "Band")
    await add_artist("https://api.discogs.com/artists/273817")
#    await add_existing_albums(file_name)

asyncio.run(main())
