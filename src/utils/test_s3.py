import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.loaders.s3_loader import S3Loader

import logging

logging.basicConfig(level=logging.INFO)


def test_s3_storage():
    """Prueba las funciones de almacenamiento en S3"""
    s3_loader = S3Loader()

    print("Archivos en S3:")
    print("\nTabla de posiciones:")
    files = s3_loader.list_files('league_table')
    if files:
        for file in files:
            print(f"- {file}")
    else:
        print("No hay archivos")

    print("\nTabla de goleadores:")
    files = s3_loader.list_files('top_scorers')
    if files:
        for file in files:
            print(f"- {file}")
    else:
        print("No hay archivos")


if __name__ == "__main__":
    test_s3_storage()