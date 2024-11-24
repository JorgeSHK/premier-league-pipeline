import psycopg2
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()

def test_database_structure():
    """Prueba la estructura y relaciones de la base de datos"""
    try:
        # Conectar a la base de datos
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        print("🔍 Probando inserción de datos y relaciones...\n")

        # 1. Insertar un equipo
        print("1. Insertando equipo de prueba...")
        cur.execute("""
            INSERT INTO teams (name) 
            VALUES ('Manchester City') 
            RETURNING team_id;
        """)
        team_id = cur.fetchone()[0]
        print(f"✅ Equipo creado con ID: {team_id}")

        # 2. Insertar un jugador
        print("\n2. Insertando jugador de prueba...")
        cur.execute("""
            INSERT INTO players (name, country, team_id)
            VALUES ('Erling Haaland', 'Norway', %s)
            RETURNING player_id;
        """, (team_id,))
        player_id = cur.fetchone()[0]
        print(f"✅ Jugador creado con ID: {player_id}")

        # 3. Insertar estadísticas del equipo
        print("\n3. Insertando estadísticas del equipo...")
        cur.execute("""
            INSERT INTO team_stats 
            (team_id, season, position, played, won, drawn, lost, goals_for, goals_against, goal_difference, points)
            VALUES (%s, '2023-2024', 1, 25, 19, 3, 3, 58, 24, 34, 60);
        """, (team_id,))
        print("✅ Estadísticas del equipo insertadas")

        # 4. Insertar estadísticas del jugador
        print("\n4. Insertando estadísticas del jugador...")
        cur.execute("""
            INSERT INTO player_stats 
            (player_id, season, goals, penalties, assists, minutes_played)
            VALUES (%s, '2023-2024', 27, 7, 5, 1800);
        """, (player_id,))
        print("✅ Estadísticas del jugador insertadas")

        # 5. Realizar una consulta compleja para verificar las relaciones
        print("\n5. Verificando relaciones entre tablas...")
        cur.execute("""
            SELECT 
                t.name as team_name,
                p.name as player_name,
                p.country,
                ps.goals,
                ps.penalties,
                ts.position as team_position,
                ts.points as team_points
            FROM teams t
            JOIN players p ON t.team_id = p.team_id
            JOIN player_stats ps ON p.player_id = ps.player_id
            JOIN team_stats ts ON t.team_id = ts.team_id
            WHERE t.name = 'Manchester City'
            AND ts.season = '2023-2024';
        """)
        
        result = cur.fetchone()
        print("\nResultados de la consulta:")
        print(f"Equipo: {result[0]}")
        print(f"Jugador: {result[1]}")
        print(f"País: {result[2]}")
        print(f"Goles: {result[3]} (Penales: {result[4]})")
        print(f"Posición del equipo: {result[5]}")
        print(f"Puntos: {result[6]}")

        # Confirmar los cambios
        conn.commit()
        print("\n✅ Todas las pruebas completadas exitosamente")

    except Exception as e:
        print(f"\n❌ Error durante las pruebas: {str(e)}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    test_database_structure()

