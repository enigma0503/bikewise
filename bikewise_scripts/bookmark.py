import json

def get_bookmark(pos):
    ts_list=[]
    with open(f'/home/itv000579/bookmark.json','r') as file:
        file_data = json.load(file)
        ls = list(file_data.values())

    for tup in ls[::-1]:
        if(tup[pos]==0):
            ts_list.append(tup)
    
    return ts_list
            

def insert_bookmark(timestamps):
    key = timestamps[2]
    with open(f'/home/itv000579/bookmark.json','r+') as file:
        file_data = json.load(file)
        file_data[key] = timestamps 
        file.seek(0)
        json.dump(file_data, file, indent = 4)
        
        
def update_bookmark(key, pos):
    with open(f'/home/itv000579/bookmark.json','r+') as file:
        file_data = json.load(file)
        file_data[key][pos] = 1 
        file.seek(0)
        json.dump(file_data, file, indent = 4)