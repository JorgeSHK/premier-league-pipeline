from data_loader import PremierLeagueLoader
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)


def test_data_loader():
    """Prueba las funciones del data loader con datos de ejemplo"""

    # Datos de prueba
    team_stats = {
        'team_name': 'Manchester City',
        'position': 1,
        'played': 25,
        'won': 19,
        'drawn': 3,
        'lost': 3,
        'goals_for': 58,
        'goals_against': 24,
        'goal_difference': 34,
        'points': 60
    }

    player_stats = {
        'name': 'Erling Haaland',
        'team_name': 'Manchester City',
        'country': 'Norway',
        'goals': 27,
        'penalties': 7
    }

    try:
        # Usar context manager para manejar la conexión
        with PremierLeagueLoader() as loader:
            print("1. Cargando estadísticas del equipo...")
            loader.load_team_stats(team_stats)
            print("✅ Estadísticas del equipo cargadas")

            print("\n2. Cargando estadísticas del jugador...")
            loader.load_player_stats(player_stats)
            print("✅ Estadísticas del jugador cargadas")

            print("\nConfirmando cambios...")
            loader.commit()
            print("✅ Datos cargados exitosamente")

    except Exception as e:
        print(f"❌ Error durante la carga de datos: {str(e)}")


if __name__ == "__main__":
    test_data_loader()