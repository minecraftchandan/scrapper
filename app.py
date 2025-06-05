import time
import requests
import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load MongoDB URI from .env
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# Setup MongoDB connection
client = MongoClient(MONGO_URI)
db = client["animeDB"]
anime_col = db["anime"]
char_col = db["characters"]

# Constants
TOTAL_PAGES = 1153
REQUESTS_PER_MIN = 60
WAIT_TIME = 60
request_count = 0

def rate_limit():
    global request_count
    request_count += 1
    if request_count >= REQUESTS_PER_MIN:
        time.sleep(WAIT_TIME)
        request_count = 0

def get_json(url):
    rate_limit()
    try:
        res = requests.get(url, timeout=10)
        if res.status_code == 200:
            return res.json()
    except:
        pass
    return None

def get_top_anime(page):
    url = f"https://api.jikan.moe/v4/top/anime?page={page}"
    data = get_json(url)
    return data.get('data', []) if data else []

def get_anime_characters(anime_id):
    characters = []
    page = 1
    while True:
        url = f"https://api.jikan.moe/v4/anime/{anime_id}/characters?page={page}"
        data = get_json(url)
        if not data or not data.get('data'):
            break
        characters.extend(data['data'])
        page += 1
    return characters

def get_character_details(char_id):
    url = f"https://api.jikan.moe/v4/characters/{char_id}"
    data = get_json(url)
    return data.get('data', {}) if data else {}

def main():
    for page in range(1, TOTAL_PAGES + 1):
        anime_list = get_top_anime(page)
        if not anime_list:
            continue

        for anime in anime_list:
            anime_id = str(anime['mal_id'])
            anime_title = anime['title']

            if not anime_col.find_one({"_id": anime_id}):
                anime_col.insert_one({"_id": anime_id, "title": anime_title})

            characters = get_anime_characters(anime_id)
            for char in characters:
                char_data = char['character']
                char_id = str(char_data['mal_id'])

                if char_col.find_one({"_id": char_id}):
                    continue

                name = char_data.get('name', 'Unknown')
                image_url = char_data.get('images', {}).get('jpg', {}).get('image_url', '')
                role = char.get('role', 'Unknown')
                details = get_character_details(char_id)

                bio = details.get('about', 'No bio').strip().replace('\n', ' ')

                char_col.insert_one({
                    "_id": char_id,
                    "name": name,
                    "anime_id": anime_id,
                    "anime_title": anime_title,
                    "role": role,
                    "image_url": image_url,
                    "bio": bio
                })

    print("✅ All anime and characters scraped successfully.")

if __name__ == "__main__":
    main()

