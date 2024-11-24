import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import logging
from datetime import datetime
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()


class S3Loader:
    """
    Clase para cargar DataFrames en formato Parquet a Amazon S3.
    """

    def __init__(self):
        """Inicializa el cliente de S3 y configura el logger"""
        self.logger = logging.getLogger(__name__)
        self.s3_client = boto3.client(
            's3',
            region_name=os.getenv('AWS_REGION'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        self.bucket_name = os.getenv('AWS_BUCKET_NAME')

    def save_to_s3(self, df: pd.DataFrame, data_type: str) -> bool:
        """
        Guarda un DataFrame como archivo Parquet en S3.

        Args:
            df: DataFrame a guardar
            data_type: Tipo de datos ('league_table' o 'top_scorers')

        Returns:
            bool: True si la carga fue exitosa, False en caso contrario
        """
        try:
            # Crear nombre de archivo con timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            s3_key = f"premier_league/{data_type}/{timestamp}.parquet"

            # Convertir DataFrame a formato Parquet en memoria
            table = pa.Table.from_pandas(df)
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)

            # Subir a S3
            self.s3_client.upload_fileobj(
                buffer,
                self.bucket_name,
                s3_key,
                ExtraArgs={'ContentType': 'application/vnd.apache.parquet'}
            )

            self.logger.info(f"Archivo guardado exitosamente en s3://{self.bucket_name}/{s3_key}")
            return True

        except Exception as e:
            self.logger.error(f"Error guardando archivo en S3: {str(e)}")
            return False

    def list_files(self, data_type: str) -> Optional[list]:
        """
        Lista los archivos disponibles para un tipo de datos.

        Args:
            data_type: Tipo de datos ('league_table' o 'top_scorers')

        Returns:
            Lista de archivos o None si hay error
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=f"premier_league/{data_type}/"
            )

            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []

        except Exception as e:
            self.logger.error(f"Error listando archivos en S3: {str(e)}")
            return None