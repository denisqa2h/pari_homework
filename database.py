import psycopg2

#Класс для подключения к базе
class PostgreSQLConnection:
    def __init__(self):
        self.host = 'postgres'
        self.port = '5432'
        self.database = 'pari_football'
        self.user = 'airflow'
        self.password = 'airflow'
        self.connection = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            print("Successfully connected to the database")
        except psycopg2.Error as e:
            print("Error connecting to the database:", e)

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Connection to the database closed")

    def execute(self, query, params=None):
        try:
            self.cursor.execute(query, params)
            print("SQL query executed successfully")
        except psycopg2.Error as e:
            print("Error executing SQL query:", e)

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()

    def commit(self):
        try:
            self.connection.commit()
            print("Changes successfully committed")
        except psycopg2.Error as e:
            print("Error committing changes to the database:", e)

    def create_tables(self):
        create_pl_fixtures_query = ''' 
            CREATE TABLE IF NOT EXISTS pl_fixtures (id serial primary key,
                        load_dttm timestamp,
                        home_team_id int,
                        home_team_name text,
                        away_team_id int,
                        away_team_name text,
                        venue text,
                        match_date timestamp,
                        current_status text)
            '''
        self.execute(create_pl_fixtures_query)

        create_pl_teams_statiistics = '''
            CREATE TABLE IF NOT EXISTS pl_team_statistics (team_id serial primary key,
                                load_dttm timestamp,
                                name text,
                                form text,
                                total_wins_of_season int,
                                total_loses_of_season int,
                                total_draws_of_season int,
                                yellow_cards int,
                                red_cards int)
            '''
        self.execute(create_pl_teams_statiistics)


        create_pl_players = '''
            CREATE TABLE IF NOT EXISTS pl_players (player_id serial primary key,
                        load_dttm timestamp,
                        player_name text,
                        games_played int,
                        goals int,
                        team_id int)
        '''
        self.execute(create_pl_players)

        create_pl_fixtures_statistcs = '''
            CREATE TABLE IF NOT EXISTS pl_fixtures_stats (int serial primary key,
                                load_dttm timestamp,
                                home_team_id int,
                                home_team_name text,
                                away_team_id int,
                                away_team_name text,
                                venue text,
                                match_date timestamp,
                                current_status text,
                                home_playername_highest_score text,
                                home_number_highest_score int,
                                away_playername_highest_score text,
                                away_number_highest_score int,
                                home_total_yellow_cards int,
                                away_total_yellow_cards int)
        '''
        self.execute(create_pl_fixtures_statistcs)

