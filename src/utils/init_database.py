import psycopg2
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()


def create_tables():
    """
    Crea las tablas necesarias en la base de datos usando un esquema normalizado.
    """
    create_tables_sql = """
    -- Tabla de equipos
    CREATE TABLE IF NOT EXISTS teams (
        team_id SERIAL PRIMARY KEY,
        name VARCHAR(100) UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Tabla de jugadores
    CREATE TABLE IF NOT EXISTS players (
        player_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        country VARCHAR(100),
        team_id INTEGER REFERENCES teams(team_id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(name, team_id)
    );

    -- Tabla de estadísticas de equipos (tabla de posiciones)
    CREATE TABLE IF NOT EXISTS team_stats (
        stat_id SERIAL PRIMARY KEY,
        team_id INTEGER REFERENCES teams(team_id),
        season VARCHAR(9),  -- e.g., "2023-2024"
        position INTEGER,
        played INTEGER,
        won INTEGER,
        drawn INTEGER,
        lost INTEGER,
        goals_for INTEGER,
        goals_against INTEGER,
        goal_difference INTEGER,
        points INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(team_id, season, updated_at)
    );

    -- Tabla de estadísticas de goleadores
    CREATE TABLE IF NOT EXISTS player_stats (
        stat_id SERIAL PRIMARY KEY,
        player_id INTEGER REFERENCES players(player_id),
        season VARCHAR(9),
        goals INTEGER DEFAULT 0,
        penalties INTEGER DEFAULT 0,
        assists INTEGER DEFAULT 0,
        minutes_played INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(player_id, season)
    );

    -- Función para actualizar el timestamp
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $$ language 'plpgsql';

    -- Triggers para actualizar timestamps
    CREATE TRIGGER update_teams_modtime
        BEFORE UPDATE ON teams
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();

    CREATE TRIGGER update_players_modtime
        BEFORE UPDATE ON players
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();

    CREATE TRIGGER update_team_stats_modtime
        BEFORE UPDATE ON team_stats
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();

    CREATE TRIGGER update_player_stats_modtime
        BEFORE UPDATE ON player_stats
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """

    try:
        # Conectar a la base de datos
        print("Conectando a la base de datos...")
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))

        # Crear un cursor y ejecutar el SQL
        print("Creando tablas...")
        cur = conn.cursor()
        cur.execute(create_tables_sql)

        # Confirmar los cambios
        conn.commit()
        print("✅ Tablas creadas exitosamente")

        # Verificar las tablas creadas
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)

        tables = cur.fetchall()
        print("\nTablas en la base de datos:")
        for table in tables:
            print(f"- {table[0]}")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error creando las tablas: {str(e)}")


if __name__ == "__main__":
    create_tables()