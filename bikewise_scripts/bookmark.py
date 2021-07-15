import json


def get_bookmark(bookmark_file, pos):
    ts_list = []
    with open(bookmark_file, 'r') as file:
        file_data = json.load(file)
        ls = list(file_data.values())

    for tup in ls[::-1]:
        if tup[pos] == 0:
            ts_list.append(tup)

    return ts_list


def insert_bookmark(bookmark_file, timestamps):
    key = timestamps[2]
    with open(bookmark_file, 'r+') as file:
        file_data = json.load(file)
        file_data[key] = timestamps
        file.seek(0)
        json.dump(file_data, file, indent=4)


def update_bookmark(bookmark_file, key, pos):
    with open(bookmark_file, 'r+') as file:
        file_data = json.load(file)
        file_data[key][pos] = 1
        file.seek(0)
        json.dump(file_data, file, indent=4)
