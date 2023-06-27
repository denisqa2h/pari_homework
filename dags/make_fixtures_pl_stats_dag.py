from database import PostgreSQLConnection
from functools import wraps
from datetime import timedelta, datetime, timezone

connection = PostgreSQLConnection()
connection.connect()

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


#Собирает табличку для отчета из 3-ех других сырых таблиц, в которых уже данные лежат, витринка
@logger
def make_pl_stats():
    query_sql = """ With top_pl_players as (
	SELECT team_id, player_name, goals
	FROM (
        SELECT team_id, player_name, goals, 
                ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY goals DESC) AS rn
        FROM pl_players) sub1
    WHERE rn = 1)
    INSERT INTO pl_fixtures_stats(load_dttm,
                home_team_id,
                home_team_name,
                away_team_id,
                away_team_name,
                venue,
                match_date,
                current_status,
                home_playername_highest_score,
                home_number_highest_score,
                away_playername_highest_score,
                away_number_highest_score,
                home_total_yellow_cards,
                away_total_yellow_cards)
    SELECT current_timestamp::timestamp,
        fix.home_team_id,
        fix.home_team_name,
        fix.away_team_id,
        fix.away_team_name,
        fix.venue,
        fix.match_date,
        fix.current_status,
        coalesce(home_plr.player_name, 'no statistic') as home_player_name,
        coalesce(home_plr.goals, 0) as home_max_goals,
        coalesce(away_plr.player_name, 'no statistic') as away_player_name,
        coalesce(away_plr.goals, 0) as away_max_goals,
        hm_team.yellow_cards,
        aw_team.yellow_cards
        from pl_fixtures fix
    LEFT JOIN pl_team_statistics hm_team on fix.home_team_id = hm_team.team_id
    LEFT JOIN pl_team_statistics aw_team on fix.away_team_id = aw_team.team_id
	LEFT JOIN top_pl_players home_plr on fix.home_team_id = home_plr.team_id
	LEFT JOIN top_pl_players away_plr on fix.away_team_id = away_plr.team_id
	WHERE NOT EXISTS (
        SELECT 1
        FROM pl_fixtures_stats
        WHERE home_team_id = fix.home_team_id
                            AND away_team_id = fix.away_team_id
                            AND venue = fix.venue)
        ORDER BY 1"""

    return query_sql

connection.execute(make_pl_stats())
connection.commit()
connection.close()