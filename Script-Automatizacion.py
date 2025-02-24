import os
import pandas as pd
from sqlalchemy import create_engine, text
from kaggle.api.kaggle_api_extended import KaggleApi

# Configuración de la conexión a SQL Server en Azure
server = 'servidorpfbna.database.windows.net'  # Cambiar por tu servidor de Azure
database = 'DEMO3'  # Cambiar por tu base de datos
username = 'adminpfnba'  # Cambiar por tu usuario
password = 'Admin123'  # Cambiar por tu contraseña

# Conexión a SQL Server usando SQLAlchemy con fast_executemany=True
engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes', fast_executemany=True)

def crear_base_de_datos_y_tablas(engine):
    """
    Crea la base de datos y las tablas necesarias si no existen.
    """
    with engine.connect() as conn:
        # Crear base de datos si no existe
        conn.execute(text(f"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{database}') CREATE DATABASE {database}"))
        print(f"Base de datos '{database}' asegurada.")

        # Cambiar a la base de datos recién creada
        conn.execute(text(f"USE {database}"))

        # SQL para crear tablas
        sql_tablas = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='common_player_info' AND xtype='U')
        CREATE TABLE common_player_info (
            person_id INT PRIMARY KEY,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            display_first_last VARCHAR(255),
            display_last_comma_first VARCHAR(255),
            display_fi_last VARCHAR(255),
            player_slug VARCHAR(255),
            birthdate VARCHAR(50),
            school VARCHAR(255),
            country VARCHAR(255),
            last_affiliation VARCHAR(255),
            height VARCHAR(50),
            weight FLOAT,
            season_exp FLOAT,
            jersey VARCHAR(50),
            position VARCHAR(50),
            rosterstatus VARCHAR(50),
            games_played_current_season_flag VARCHAR(50),
            team_id INT,
            team_name VARCHAR(255),
            team_abbreviation VARCHAR(50),
            team_code VARCHAR(50),
            team_city VARCHAR(255),
            playercode VARCHAR(50),
            from_year FLOAT,
            to_year FLOAT,
            dleague_flag VARCHAR(50),
            nba_flag VARCHAR(50),
            games_played_flag VARCHAR(50),
            draft_year VARCHAR(50),
            draft_round VARCHAR(50),
            draft_number VARCHAR(50),
            greatest_75_flag VARCHAR(50)
        );
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='draft_combine_stats' AND xtype='U')
        CREATE TABLE draft_combine_stats (
            season INT,
            player_id INT PRIMARY KEY,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            player_name VARCHAR(255),
            position VARCHAR(50),
            height_wo_shoes FLOAT,
            height_wo_shoes_ft_in VARCHAR(50),
            height_w_shoes FLOAT,
            height_w_shoes_ft_in VARCHAR(50),
            weight FLOAT,
            wingspan FLOAT,
            wingspan_ft_in VARCHAR(50),
            standing_reach FLOAT,
            standing_reach_ft_in VARCHAR(50),
            body_fat_pct FLOAT,
            hand_length FLOAT,
            hand_width FLOAT,
            standing_vertical_leap FLOAT,
            max_vertical_leap FLOAT,
            lane_agility_time FLOAT,
            modified_lane_agility_time FLOAT,
            three_quarter_sprint FLOAT,
            bench_press FLOAT,
            spot_fifteen_corner_left VARCHAR(50),
            spot_fifteen_break_left VARCHAR(50),
            spot_fifteen_top_key VARCHAR(50),
            spot_fifteen_break_right VARCHAR(50),
            spot_fifteen_corner_right VARCHAR(50),
            spot_college_corner_left VARCHAR(50),
            spot_college_break_left VARCHAR(50),
            spot_college_top_key VARCHAR(50),
            spot_college_break_right VARCHAR(50),
            spot_college_corner_right VARCHAR(50),
            spot_nba_corner_left VARCHAR(50),
            spot_nba_break_left VARCHAR(50),
            spot_nba_top_key VARCHAR(50),
            spot_nba_break_right VARCHAR(50),
            spot_nba_corner_right VARCHAR(50),
            off_drib_fifteen_break_left VARCHAR(50),
            off_drib_fifteen_top_key VARCHAR(50),
            off_drib_fifteen_break_right VARCHAR(50),
            off_drib_college_break_left VARCHAR(50),
            off_drib_college_top_key VARCHAR(50),
            off_drib_college_break_right VARCHAR(50),
            on_move_fifteen VARCHAR(50),
            on_move_college VARCHAR(50)
        );
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='draft_history' AND xtype='U')
        CREATE TABLE draft_history (
            person_id INT PRIMARY KEY,
            player_name VARCHAR(255),
            season INT,
            round_number INT,
            round_pick INT,
            overall_pick INT,
            draft_type VARCHAR(50),
            team_id INT,
            team_city VARCHAR(255),
            team_name VARCHAR(255),
            team_abbreviation VARCHAR(50),
            organization VARCHAR(255),
            organization_type VARCHAR(50),
            player_profile_flag INT
        );
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='officials' AND xtype='U')
        CREATE TABLE officials (
            game_id INT PRIMARY KEY,
            official_id INT,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            jersey_num FLOAT
        );
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='player' AND xtype='U')
        CREATE TABLE player (
            id INT PRIMARY KEY,
            full_name VARCHAR(255),
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            is_active INT
        );
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='inactive_players' AND xtype='U')
        CREATE TABLE inactive_players (
            game_id INT PRIMARY KEY,
            player_id INT,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            jersey_num FLOAT,
            team_id INT,
            team_city VARCHAR(255),
            team_name VARCHAR(255),
            team_abbreviation VARCHAR(50)
        );
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='game' AND xtype='U')
        CREATE TABLE game (
            season_id INT PRIMARY KEY,
            team_id_home INT,
            team_abbreviation_home VARCHAR(255),
            team_name_home VARCHAR(255),
            game_id INT,
            game_date DATE,
            matchup_home VARCHAR(255),
            wl_home VARCHAR(255),
            min INT,
            fgm_home FLOAT,
            fga_home FLOAT,
            fg_pct_home FLOAT,
            fg3m_home FLOAT,
            fg3a_home FLOAT,
            fg3_pct_home FLOAT,
            ftm_home FLOAT,
            fta_home FLOAT,
            ft_pct_home FLOAT,
            oreb_home FLOAT,
            dreb_home FLOAT,
            reb_home FLOAT,
            ast_home FLOAT,
            stl_home FLOAT,
            tov_home FLOAT,
            pf_home FLOAT,
            plus_minus_home INT,
            video_available_home INT,
            team_id_away INT,
            team_abbreviation_away VARCHAR(255),
            team_name_away VARCHAR(255),
            matchup_away VARCHAR(255),
            wl_away VARCHAR(255),
            fgm_away FLOAT,
            fga_away FLOAT,
            fg_pct_away FLOAT,
            fg3m_away FLOAT,
            fg3a_away FLOAT,
            fg3_pct_away FLOAT,
            ftm_away FLOAT,
            fta_away FLOAT,
            ft_pct_away FLOAT,
            oreb_away FLOAT,
            dreb_away FLOAT,
            reb_away FLOAT,
            ast_away FLOAT,
            stl_away FLOAT,
            blk_away FLOAT,
            tov_away FLOAT,
            pf_away FLOAT,
            pts_away FLOAT,
            plus_minus_away INT,
            video_available_away INT,
            season_type VARCHAR(255)
        );
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='game_info' AND xtype='U')
        CREATE TABLE game_info (
            game_id INT PRIMARY KEY,
            game_date NVARCHAR(100),
            attendance FLOAT,
            game_time NVARCHAR(100)
        );
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='game_summary' AND xtype='U')
        CREATE TABLE game_summary (
            game_date_est DATE,
            game_sequence FLOAT,
            game_id INT PRIMARY KEY,
            game_status_id INT,
            game_status_text VARCHAR(100),
            gamecode NVARCHAR(255),
            home_team_id INT,
            visitor_team_id INT,
            season VARCHAR(100),
            live_period INT,
            natl_tv_broadcaster_abbreviation VARCHAR(255),
            live_period_time_bcast NVARCHAR(100),
            wh_status INT
        );
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='line_score' AND xtype='U')
        CREATE TABLE line_score (
            game_date_est DATE,
            game_sequence INT,
            game_id INT PRIMARY KEY,
            team_id_home INT,
            team_abbreviation_home VARCHAR(10),
            team_city_name_home NVARCHAR(255),
            team_nickname_home VARCHAR(100),
            team_wins_losses_home NVARCHAR(100),
            pts_qtr1_home FLOAT,
            pts_qtr2_home FLOAT,
            pts_qtr3_home FLOAT,
            pts_home FLOAT,
            team_id_away INT,
            team_abbreviation_away VARCHAR(10),
            team_city_name_away NVARCHAR(255),
            team_nickname_away VARCHAR(100),
            team_wins_losses_away NVARCHAR(100),
            pts_qtr1_away FLOAT,
            pts_qtr2_away FLOAT,
            pts_qtr3_away FLOAT,
            pts_qtr4_away FLOAT,
            pts_away FLOAT
        );
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='other_stats' AND xtype='U')
        CREATE TABLE other_stats (
            game_id INT PRIMARY KEY,
            league_id INT,
            team_id_home INT,
            team_abbreviation_home VARCHAR(100),
            team_city_home VARCHAR(100),
            pts_paint_home INT,
            pts_2nd_chance_home INT,
            pts_fb_home INT,
            largest_lead_home INT,
            lead_changes INT,
            times_tied INT,
            team_turnovers_home FLOAT,
            total_turnovers_home FLOAT,
            team_rebounds_home FLOAT,
            pts_off_to_home FLOAT,
            team_id_away INT,
            team_abbreviation_away VARCHAR(100),
            team_city_away VARCHAR(100),
            pts_paint_away INT,
            pts_2nd_chance_away INT,
            pts_fb_away INT,
            largest_lead_away INT,
            team_turnovers_away FLOAT,
            total_turnovers_away FLOAT,
            team_rebounds_away FLOAT,
            pts_off_to_away FLOAT
        );
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='team' AND xtype='U')
        CREATE TABLE team (
            id INT PRIMARY KEY,
            full_name VARCHAR(100),
            abbreviation VARCHAR(100),
            nickname VARCHAR(100),
            city VARCHAR(100),
            state VARCHAR(100),
            year_founded FLOAT
        );
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='team_details' AND xtype='U')
        CREATE TABLE team_details (
            team_id INT PRIMARY KEY,
            abbreviation VARCHAR(100),
            nickname VARCHAR(100),
            yearfounded FLOAT,
            city VARCHAR(100),
            arena VARCHAR(100),
            arenacapacity FLOAT,
            owner NVARCHAR(100),
            generalmanager NVARCHAR(100),
            headcoach NVARCHAR(100),
            dleagueaffiliation NVARCHAR(100),
            facebook NVARCHAR(100),
            instagram NVARCHAR(100),
            twitter NVARCHAR(100)
        );
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='team_history' AND xtype='U')
        CREATE TABLE team_history (
            team_id INT PRIMARY KEY,
            city VARCHAR(100),
            nickname VARCHAR(100),
            year_founded INT,
            year_active_till INT
        );
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='play_by_play' AND xtype='U')
        CREATE TABLE play_by_play (
            game_id INT,
            period INT,
            wctimestring VARCHAR(255),
            pctimestring VARCHAR(255),
            score VARCHAR(255),
            scoremargin VARCHAR(255),
            person1type INT,
            player1_id INT,
            player1_team_id INT
        );
        """
        
        # Ejecutar las sentencias SQL para crear las tablas
        for statement in sql_tablas.split(";"):
            if statement.strip():
                conn.execute(text(statement))
        print("Tablas aseguradas.")

def bulk_insert(engine, table_name, df, chunk_size=1000):
    """
    Inserta datos en la tabla de SQL Server en partes más pequeñas.
    """
    with engine.connect() as conn:
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            try:
                chunk.to_sql(table_name, conn, if_exists='append', index=False)
                print(f"Chunk {i // chunk_size + 1} cargado en la tabla {table_name}.")
            except Exception as e:
                print(f"Error al cargar chunk {i // chunk_size + 1}: {e}")
                conn.rollback()

def main():
    # Configuración de Kaggle y dataset
    kaggle_dataset = "wyattowalsh/basketball"  # Reemplaza con el nombre del dataset en Kaggle
    base_path = r"C:\PF\csv"

    # Crear base de datos y tablas si no existen
    crear_base_de_datos_y_tablas(engine)

    # Aquí iría el resto del código para descargar y cargar datos a la base
    archivos = {"common_player_info.csv": "common_player_info",
                "draft_combine_stats.csv": "draft_combine_stats",
                "draft_history.csv": "draft_history",
                "officials.csv": "officials",
                "player.csv": "player",
                "inactive_players.csv": "inactive_players",
                "game.csv": "game",
                "game_info.csv": "game_info",
                "game_summary.csv": "game_summary",
                "line_score.csv": "line_score",
                "other_stats.csv": "other_stats",
                "team.csv": "team",
                "team_details.csv": "team_details",
                "team_history.csv": "team_history",
                "play_by_play.csv": "play_by_play"}

    for archivo, tabla in archivos.items():
        # Cargar los archivos CSV
        file_path = os.path.join(base_path, archivo)
        if not os.path.exists(file_path):
            print(f"Archivo no encontrado: {file_path}")
            continue

        df = pd.read_csv(file_path)

        # Si el archivo es play_by_play.csv, realizar las transformaciones necesarias
        if archivo == "play_by_play.csv":
            # Eliminar las filas con valores nulos en la columna 'score'
            df = df.dropna(subset=['score'])

            # Seleccionar solo las columnas especificadas
            columns_to_keep = ['game_id', 'period', 'wctimestring', 'pctimestring', 'score', 'scoremargin', 'person1type', 'player1_id', 'player1_team_id']
            df = df[columns_to_keep]

        # Cargar los datos a la tabla SQL en partes más pequeñas usando bulk_insert
        bulk_insert(engine, tabla, df, chunk_size=1000)

if __name__ == "__main__":
    main()