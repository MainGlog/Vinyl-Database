

def add_new_line_chars(file_name):
    file = open(file_name)

    statement = ""
    for line in file:
        line = line.replace("\n", "")
        statement = line.replace("),", "),\n")

    file.close()

    print(statement)

    with open(file_name, "w") as file:
        file.write(statement)
        file.close()

def extract_artists(file_name, artist_column_number):
    file = open(file_name)
    artists = []

    for line in file:
        artists.append(line.split("'")[artist_column_number - 1])
    print(artists)
    
def format_table_list(file_name):
    file = open(file_name)
    
    lines = []
    counter = 1
    for line in file:
        table_name = line.split("EXISTS ")[1]
        new_line = "(" + str(counter) + ", '" + table_name
        new_line = new_line.split(";")[0] + "'),\n"
        lines.append(new_line)
        counter += 1
    
    file.close()

    file = open(file_name, "w")
    file.writelines(lines)


        

        

def append_artists(artists_file_name, albums_file_name):
    artists_file = open(artists_file_name)
    albums_file = open(albums_file_name)

    artists = []
    
    for line in artists_file:
        artists.append("(" + line)

    updated_records = []
    index = 0
    for line in albums_file:
        updated_records.append(artists[index] + line + "),") 
        index += 1
    print(updated_records)



    




file_name = "temp.txt"


# add_new_line_chars(file_name)
# extract_artists(file_name, 2)
# format_table_list(file_name)
# get_release_dates(file_name)
# append_artists(file_name, "Albums.txt")