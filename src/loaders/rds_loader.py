import pandas as pd
from sqlalchemy import create_engine, text
import logging
from typing import Optional


class RDSLoader:
    """
    Clase para cargar DataFrames a Amazon RDS (PostgreSQL).
    """

    def __init__(self, connection_string: str):
        """
        Inicializa el loader de RDS.

        Args:
            connection_string: String de conexión a la base de datos
                Format: postgresql://user:password@host:port/database
        """
        self.logger = logging.getLogger(__name__)
        self.connection_string = connection_string
        self.engine = create_engine(connection_string)

    def upload_to_rds(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace') -> bool:
        """
        Carga un DataFrame a una tabla en RDS.

        Args:
            df: DataFrame a cargar
            table_name: Nombre de la tabla
            if_exists: Comportamiento si la tabla existe ('fail', 'replace', 'append')

        Returns:
            bool: True si la carga fue exitosa, False en caso contrario
        """
        try:
            # Crear conexión
            with self.engine.connect() as connection:
                # Cargar datos
                df.to_sql(
                    name=table_name,
                    con=connection,
                    if_exists=if_exists,
                    index=False
                )

            self.logger.info(f"Datos cargados exitosamente en la tabla {table_name}")
            return True

        except Exception as e:
            self.logger.error(f"Error cargando datos en la tabla {table_name}: {str(e)}")
            return False

    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """
        Ejecuta una consulta SQL y retorna los resultados.

        Args:
            query: Consulta SQL a ejecutar

        Returns:
            DataFrame con los resultados o None si hay error
        """
        try:
            with self.engine.connect() as connection:
                result = pd.read_sql_query(text(query), connection)
            return result
        except Exception as e:
            self.logger.error(f"Error ejecutando query: {str(e)}")
            return None