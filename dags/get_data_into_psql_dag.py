from database import PostgreSQLConnection
from datetime import timedelta, datetime, timezone
from functools import wraps

import os
import json
import psycopg2
import requests


headers = {
	"X-RapidAPI-Key": "48a5cd9d12msh33a4e6acd087504p1c8131jsnd1b6952676a4",
	"X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
}

connection = PostgreSQLConnection()
connection.connect()
connection.create_tables()

#Логер функций
def logger(func):

    @wraps(func)
    def inner(*args, **kwargs):
        called_at = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))
        print(f">>> Running {func.__name__!r} function. Logged at {called_at}")
        to_execute = func(*args, **kwargs)
        print(f">>> Function: {func.__name__!r} executed. Logged at {called_at}")
        return to_execute

    return inner

#Функция делает запрос по api
@logger
def get_data(url, querystring, headers, team_id):
    if team_id is not None:
        querystring["team"] = team_id
    response = requests.get(url, headers=headers, params=querystring)
    data_json = response.json()
    return data_json

#Обращается к апи встреч будущих, льет в сыром виде в PSQL, табличку pl_fixtures, возвращает список id команд
@logger
def get_fixtures(connection):
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    querystring = {"season": 2023, "league": 39, "status":"TBD-NS", "timezone":"Europe/Moscow", "next":5}
    data_json = get_data(url, querystring, headers, None)
    list_team_ids = []
    for i in data_json['response']:
        id_team_1 = i['teams']['home']['id']
        team_1 = i['teams']['home']['name']
        id_team_2 = i['teams']['away']['id']
        team_2 = i['teams']['away']['name']
        venue = i['fixture']['venue']['name']
        match_date = i['fixture']['date']
        match_date = datetime.strptime(match_date, '%Y-%m-%dT%H:%M:%S%z').replace(tzinfo=None)
        current_status = i['fixture']['status']['long']
        list_team_ids.extend((id_team_1, id_team_2))
        query_sql = """INSERT INTO pl_fixtures (load_dttm, home_team_id, home_team_name, away_team_id, away_team_name, venue, match_date, current_status)
                         SELECT current_timestamp::timestamp, %s, %s, %s, %s, %s, %s, %s 
                         WHERE NOT EXISTS
                            ( SELECT 1
                                FROM pl_fixtures
                                WHERE home_team_id = %s
                                AND away_team_id = %s
                                AND venue = %s )"""
        connection.execute(query_sql, (id_team_1, team_1, id_team_2, team_2, venue, match_date, current_status, id_team_1, id_team_2, venue))
    connection.commit()
    list_team_ids = list(dict.fromkeys(list_team_ids))
    return list_team_ids

#Считает сумму значений по ключу в словаре (JSON)
@logger
def get_total_sum(data):
    total_sum = 0
    if isinstance(data, dict):
        for key, value in data.items():
            if key == 'total' and value is not None:
                total_sum += value
            else:
                total_sum += get_total_sum(value)
    elif isinstance(data, list):
        for item in data:
            total_sum += get_total_sum(item)
    return total_sum

#Обращается к апи, собирает статистику по командам, которые будут играть, льет в сыром виде в PSQL, в табличку pl_team_statistics
@logger
def get_team_statistics(team_id, connection):
    url = "https://api-football-v1.p.rapidapi.com/v3/teams/statistics"
    querystring = {"league": 39, "season": 2022}
    data_json = get_data(url, querystring, headers, team_id)
    data_json = data_json['response']
    name = data_json['team']['name']
    form = data_json['form']
    total_wins_of_season = data_json['fixtures']['wins']['total']
    total_loses_of_season = data_json['fixtures']['loses']['total']
    total_draws_of_season = data_json['fixtures']['draws']['total']
    total_yellow_cards = get_total_sum(data_json['cards']['yellow'])
    total_red_cards = get_total_sum(data_json['cards']['red'])
    # lst.extend((team_id, name, form, total_wins_of_season, total_loses_of_season, total_draws_of_season, total_yellow_cards, total_red_cards))
    query_sql = """INSERT INTO pl_team_statistics (team_id, load_dttm, name, form, total_wins_of_season, total_loses_of_season, total_draws_of_season, yellow_cards, red_cards)
                          SELECT %s, current_timestamp::timestamp, %s, %s, %s, %s, %s, %s, %s
                          WHERE NOT EXISTS
                             ( SELECT 1
                                 FROM pl_team_statistics
                                 WHERE team_id = %s )"""
    connection.execute(query_sql, (team_id, name, form, total_wins_of_season, total_loses_of_season, total_draws_of_season, total_yellow_cards, total_red_cards, team_id))
    connection.commit()

#Обращается к апи, собирает информацию об игроках команд, которые будут играть, льет в сыром виде в PSQL, в табличку pl_players
@logger
def get_players(team_id, connection):
    url = "https://api-football-v1.p.rapidapi.com/v3/players"
    querystring = {"league": 39, "season": 2022}
    data_json = get_data(url, querystring, headers, team_id)
    for i in data_json['response']:
        player_id = i['player']['id']
        player_name = i['player']['name']
        games_played = i['statistics'][0]['games']['appearences']
        goals = i['statistics'][0]['goals']['total']
        query_sql = """INSERT INTO pl_players (player_id, load_dttm, player_name, games_played, goals, team_id)
                          SELECT %s, current_timestamp::timestamp, %s, %s, coalesce(%s, 0), %s
                          WHERE NOT EXISTS
                             ( SELECT 1
                                 FROM pl_players
                                 WHERE player_id = %s ) """
        connection.execute(query_sql, (player_id, player_name, games_played, goals, team_id, player_id))
    connection.commit()


#MAIN, самая главная такая
@logger
def main():
    ids_teams = get_fixtures(connection)
    for i in ids_teams:
        get_team_statistics(i, connection)
        get_players(i, connection)
        print(f'Successfully loaded into the database by command ID: {i}')    
main()


connection.close()