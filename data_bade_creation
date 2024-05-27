import pandas as pd
from pymongo import MongoClient
client = MongoClient('127.0.0.1', 27017)
db = client.football

csv_file_1 = r'premier-league_2022-2023_fixture_data.csv'
csv_file_2 = r'premier-league_2022-2023_player_data.csv'
matches_df = pd.read_csv(csv_file_1)
players_df = pd.read_csv(csv_file_2)
json_output_1 = r'premier-league_2022-2023_fixture_data.json'
json_output_2 = r'premier-league_2022-2023_players_data.json'
output_1 = matches_df.to_json(json_output_1)
output_2 = players_df.to_json(json_output_2)
data_matches = matches_df.to_dict(orient='records')
data_players = players_df.to_dict(orient='records')
db.matches.insert_many(data_matches)
db.players.insert_many(data_players)
