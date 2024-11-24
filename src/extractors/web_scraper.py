import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from typing import Optional, Dict, List
from src.loaders.data_loader import PremierLeagueLoader
from src.loaders.s3_loader import S3Loader


class PremierLeagueScraper:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.urls = {
            'league_table': 'https://www.bbc.com/sport/football/premier-league/table',
            'top_scorers': 'https://www.worldfootball.net/goalgetter/eng-premier-league-2023-2024/'
        }
        self.loader = PremierLeagueLoader()
        self.s3_loader = S3Loader()

    def get_league_table(self) -> Optional[pd.DataFrame]:
        """Extrae la tabla de posiciones"""
        try:
            response = requests.get(self.urls['league_table'], headers=self.headers)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.find("table", class_="ssrcss-14j0ip6-Table e3bga5w5")

            if not table:
                self.logger.error("No se encontró la tabla de posiciones")
                return None

            # Extraer headers
            headers = [th.text for th in table.find_all('th')]

            # Extraer datos
            rows = []
            for row in table.find_all('tr')[1:]:
                row_data = [td.text for td in row.find_all('td')]
                if row_data:
                    rows.append(row_data)

            # Crear DataFrame
            df = pd.DataFrame(rows, columns=headers)

            # Limpieza básica
            if 'Form, Last 6 games, Oldest first' in df.columns:
                df = df.drop(["Form, Last 6 games, Oldest first"], axis=1)

            return df

        except requests.RequestException as e:
            self.logger.error(f"Error al hacer la petición HTTP: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Error inesperado: {str(e)}")
            return None

    def get_top_scorers(self) -> Optional[pd.DataFrame]:
        """Extrae la tabla de goleadores"""
        try:
            response = requests.get(self.urls['top_scorers'], headers=self.headers)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.find("table", class_="standard_tabelle")

            if not table:
                self.logger.error("No se encontró la tabla de goleadores")
                return None

            # Extraer datos
            rows = []
            first_row = True

            for row in table.find_all('tr'):
                if first_row:
                    first_row = False
                    continue

                cells = row.find_all('td')
                if len(cells) >= 4:
                    try:
                        position = cells[0].text.strip().replace('.', '')
                        player = cells[1].text.strip()
                        country = cells[3].text.strip()
                        team = cells[4].text.strip().replace('\n', '').strip()
                        goals_text = cells[5].text.strip()

                        goals = int(goals_text.split('(')[0].strip())
                        penalties = 0
                        if '(' in goals_text:
                            penalties = int(goals_text.split('(')[1].replace(')', '').strip())

                        rows.append([
                            position,
                            player,
                            country,
                            team,
                            goals,
                            penalties
                        ])
                    except Exception as e:
                        self.logger.warning(f"Error procesando fila: {e}")
                        continue

            # Crear DataFrame
            df = pd.DataFrame(rows, columns=['Posición', 'Jugador', 'País', 'Equipo', 'Goles', 'Penales'])
            return df

        except requests.RequestException as e:
            self.logger.error(f"Error al hacer la petición HTTP: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Error inesperado: {str(e)}")
            return None

    def extract_and_load_league_table(self) -> bool:
        try:
            df = self.get_league_table()
            if df is None:
                return False

            # Guardar en S3
            self.logger.info("Guardando tabla de posiciones en S3...")
            if self.s3_loader.save_to_s3(df, 'league_table'):
                self.logger.info("Tabla de posiciones guardada en S3")
            else:
                self.logger.error("Error guardando tabla de posiciones en S3")

            # Cargar en PostgreSQL
            for _, row in df.iterrows():
                team_stats = {
                    'team_name': row['Team'],
                    'position': int(row['Position']),
                    'played': int(row['Played']),
                    'won': int(row['Won']),
                    'drawn': int(row['Drawn']),
                    'lost': int(row['Lost']),
                    'goals_for': int(row['Goals For']),
                    'goals_against': int(row['Goals Against']),
                    'goal_difference': int(row['Goal Difference']),
                    'points': int(row['Points'])
                }
                self.loader.load_team_stats(team_stats)

            self.loader.commit()
            self.logger.info("Tabla de posiciones cargada exitosamente")
            return True

        except Exception as e:
            self.logger.error(f"Error procesando tabla de posiciones: {str(e)}")
            self.loader.rollback()
            return False

    def extract_and_load_top_scorers(self) -> bool:
        try:
            df = self.get_top_scorers()
            if df is None:
                return False

            # Guardar en S3
            self.logger.info("Guardando tabla de goleadores en S3...")
            if self.s3_loader.save_to_s3(df, 'top_scorers'):
                self.logger.info("Tabla de goleadores guardada en S3")
            else:
                self.logger.error("Error guardando tabla de goleadores en S3")

            # Cargar en PostgreSQL
            for _, row in df.iterrows():
                player_stats = {
                    'name': row['Jugador'],
                    'team_name': row['Equipo'],
                    'country': row['País'],
                    'goals': int(row['Goles']),
                    'penalties': int(row['Penales'])
                }
                self.loader.load_player_stats(player_stats)

            self.loader.commit()
            self.logger.info("Tabla de goleadores cargada exitosamente")
            return True

        except Exception as e:
            self.logger.error(f"Error procesando tabla de goleadores: {str(e)}")
            self.loader.rollback()
            return False

    def update_all_data(self):
        """Actualiza todos los datos"""
        try:
            print("Iniciando actualización de datos...")

            print("\n1. Actualizando tabla de posiciones...")
            if self.extract_and_load_league_table():
                print("✅ Tabla de posiciones actualizada")
            else:
                print("❌ Error actualizando tabla de posiciones")

            print("\n2. Actualizando tabla de goleadores...")
            if self.extract_and_load_top_scorers():
                print("✅ Tabla de goleadores actualizada")
            else:
                print("❌ Error actualizando tabla de goleadores")

            print("\n✅ Proceso de actualización completado")

        except Exception as e:
            print(f"\n❌ Error durante la actualización: {str(e)}")
        finally:
            if hasattr(self, 'loader'):
                self.loader.__exit__(None, None, None)