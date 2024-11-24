import os
import sys

# Agregar el directorio raíz al PYTHONPATH
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_dir)

from src.extractors.web_scraper import PremierLeagueScraper
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def main():
    """Función principal que ejecuta el pipeline de datos"""
    try:
        # Inicializar el scraper
        scraper = PremierLeagueScraper()

        # Ejecutar la actualización
        scraper.update_all_data()

    except Exception as e:
        logging.error(f"Error en el pipeline: {str(e)}")


if __name__ == "__main__":
    main()