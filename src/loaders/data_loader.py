import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import logging
from typing import List, Dict, Any
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

class PremierLeagueLoader:
    """
    Clase para cargar datos de la Premier League en la base de datos.
    Maneja la inserción y actualización de equipos, jugadores y estadísticas.
    """

    def __init__(self):
        """Inicializa la conexión a la base de datos"""
        self.conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        self.cur = self.conn.cursor()
        self.logger = logging.getLogger(__name__)
        self.season = "2023-2024"  # Temporada actual

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cierra las conexiones al salir del contexto"""
        self.cur.close()
        self.conn.close()

    def load_team(self, team_name: str) -> int:
        """
        Carga o actualiza un equipo y devuelve su ID.

        Args:
            team_name: Nombre del equipo

        Returns:
            ID del equipo en la base de datos
        """
        try:
            # Intentar insertar el equipo si no existe
            self.cur.execute("""
                INSERT INTO teams (name)
                VALUES (%s)
                ON CONFLICT (name) DO UPDATE 
                SET updated_at = CURRENT_TIMESTAMP
                RETURNING team_id;
            """, (team_name,))

            team_id = self.cur.fetchone()[0]
            return team_id

        except Exception as e:
            self.logger.error(f"Error cargando equipo {team_name}: {str(e)}")
            raise

    def load_player(self, player_data: Dict[str, Any]) -> int:
        """
        Carga o actualiza un jugador y devuelve su ID.

        Args:
            player_data: Diccionario con datos del jugador
                {
                    'name': str,
                    'team_name': str,
                    'country': str
                }

        Returns:
            ID del jugador en la base de datos
        """
        try:
            # Primero obtener el team_id
            team_id = self.load_team(player_data['team_name'])

            # Insertar o actualizar jugador
            self.cur.execute("""
                INSERT INTO players (name, country, team_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (name, team_id) DO UPDATE 
                SET 
                    country = EXCLUDED.country,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING player_id;
            """, (player_data['name'], player_data['country'], team_id))

            player_id = self.cur.fetchone()[0]
            return player_id

        except Exception as e:
            self.logger.error(f"Error cargando jugador {player_data['name']}: {str(e)}")
            raise

    def load_team_stats(self, stats_data: Dict[str, Any]):
        """
        Carga o actualiza las estadísticas de un equipo.

        Args:
            stats_data: Diccionario con estadísticas del equipo
                {
                    'team_name': str,
                    'position': int,
                    'played': int,
                    'won': int,
                    'drawn': int,
                    'lost': int,
                    'goals_for': int,
                    'goals_against': int,
                    'goal_difference': int,
                    'points': int
                }
        """
        try:
            team_id = self.load_team(stats_data['team_name'])

            self.cur.execute("""
                INSERT INTO team_stats 
                (team_id, season, position, played, won, drawn, lost, 
                 goals_for, goals_against, goal_difference, points)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (team_id, season, updated_at) DO UPDATE 
                SET 
                    position = EXCLUDED.position,
                    played = EXCLUDED.played,
                    won = EXCLUDED.won,
                    drawn = EXCLUDED.drawn,
                    lost = EXCLUDED.lost,
                    goals_for = EXCLUDED.goals_for,
                    goals_against = EXCLUDED.goals_against,
                    goal_difference = EXCLUDED.goal_difference,
                    points = EXCLUDED.points,
                    updated_at = CURRENT_TIMESTAMP;
            """, (
                team_id, self.season, stats_data['position'],
                stats_data['played'], stats_data['won'],
                stats_data['drawn'], stats_data['lost'],
                stats_data['goals_for'], stats_data['goals_against'],
                stats_data['goal_difference'], stats_data['points']
            ))

        except Exception as e:
            self.logger.error(f"Error cargando estadísticas del equipo {stats_data['team_name']}: {str(e)}")
            raise

    def load_player_stats(self, stats_data: Dict[str, Any]):
        """
        Carga o actualiza las estadísticas de un jugador.

        Args:
            stats_data: Diccionario con estadísticas del jugador
                {
                    'name': str,
                    'team_name': str,
                    'goals': int,
                    'penalties': int,
                    'country': str
                }
        """
        try:
            # Primero cargar/actualizar el jugador
            player_data = {
                'name': stats_data['name'],
                'team_name': stats_data['team_name'],
                'country': stats_data['country']
            }
            player_id = self.load_player(player_data)

            # Luego insertar/actualizar sus estadísticas
            self.cur.execute("""
                INSERT INTO player_stats 
                (player_id, season, goals, penalties)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (player_id, season) DO UPDATE 
                SET 
                    goals = EXCLUDED.goals,
                    penalties = EXCLUDED.penalties,
                    updated_at = CURRENT_TIMESTAMP;
            """, (
                player_id, self.season,
                stats_data['goals'], stats_data['penalties']
            ))

        except Exception as e:
            self.logger.error(f"Error cargando estadísticas del jugador {stats_data['name']}: {str(e)}")
            raise

    def commit(self):
        """Confirma los cambios en la base de datos"""
        self.conn.commit()

    def rollback(self):
        """Revierte los cambios en caso de error"""
        self.conn.rollback()