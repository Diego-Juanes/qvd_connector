from typing import Iterator, List, Any
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType, DataType
from pyspark.sql import Row
from pyspark.sql.session import SparkSession
import glob
from pyqvd import QvdTable
import pandas as pd
from datetime import datetime


# ==============================================================================
# 1. FUNCIONES DE UTILIDAD PARA INFERENCIA DE ESQUEMA
# ==============================================================================
def map_qvd_metadata_to_spark_schema(qvd_table_instance: QvdTable) -> StructType:
    """
    Crea el esquema de Spark a partir de la metadata de campo de PyQvd,
    mapeando tipos Qlik a tipos Spark de forma precisa.
    """
    fields = []

    for field_name, field_def in qvd_table_instance.field_defs.items():
        spark_type = StringType() # Default si no se identifica nada más específico

        # 1. Analizar el tipo de campo (INTEGER, REAL)
        field_type = field_def['field_type']

        # 2. Analizar el tipo de valor (NUMERIC, STRING)
        value_type = field_def['value_type']

        if value_type == 'NUMERIC':
            fmt = field_def.get('format', {})

            # Es un valor numérico. ¿Es una fecha/hora?
            if fmt.get('type') in ('DATE', 'TIME', 'TIMESTAMP', 'INTERVAL'):
                spark_type = TimestampType()
            # ¿Es un entero o un real?
            elif field_type == 'INTEGER':
                spark_type = LongType()
            elif field_type == 'REAL':
                spark_type = DoubleType()
            else:
                spark_type = DoubleType() # Por defecto a Double si es numérico pero ambiguo

        elif value_type == 'STRING':
            spark_type = StringType() # Dejar como string si es puramente texto

        fields.append(StructField(field_name, spark_type, nullable=True))

    return StructType(fields)

# ==============================================================================
# 2. CLASES DEL CONECTOR PYSPARK DSv2
# ==============================================================================

class QvdFilePartition(InputPartition):
    """Representa una partición (un archivo QVD completo) a leer."""
    def __init__(self, file_path: str):
        self.file_path = file_path

class QvdDataSourceReader(DataSourceReader):
    """Implementa la lógica de lectura (batch y incremental) y conversión de QVD a Spark."""
    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options
        self.enable_arrow = self.options.get("enableArrow", "false").lower() == "true"

        # Lógica de Inicialización Incremental
        self.incremental_column = self.options.get("incrementalColumn")
        self.last_load_timestamp_str = self.options.get("lastLoadTimestamp")
        self.last_load_timestamp = None

        if self.last_load_timestamp_str:
            try:
                # Intenta parsear la cadena como datetime para una comparación precisa
                self.last_load_timestamp = datetime.strptime(self.last_load_timestamp_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                print(f"Advertencia: El formato de 'lastLoadTimestamp' ({self.last_load_timestamp_str}) no es válido (Debe ser YYYY-MM-DD HH:MM:SS). La carga incremental será ignorada.")


    def plan_partitions(self) -> List[QvdFilePartition]:
        """Divide el trabajo. Cada archivo QVD es una partición."""
        file_path_pattern = self.options.get("path")
        if not file_path_pattern:
            raise ValueError("La opción 'path' es obligatoria.")

        file_paths = glob.glob(file_path_pattern)
        return [QvdFilePartition(path) for path in file_paths]

    def read(self, partition: QvdFilePartition) -> Iterator[Row]:
        """
        Carga el QVD completo en el worker, aplica el filtro incremental y lo convierte a Rows.
        """
        file_path = partition.file_path

        try:
            qvd_table = QvdTable.from_qvd(file_path)
            df_pandas = qvd_table.to_pandas()

            # 1. Lógica de Filtrado Incremental (Aplicada en Pandas para eficiencia)
            if self.incremental_column and self.last_load_timestamp:
                print(f"Aplicando filtro incremental en {self.incremental_column} > {self.last_load_timestamp} en archivo {file_path}")

                # Asegura que la columna de Pandas sea de tipo datetime para la comparación
                if df_pandas[self.incremental_column].dtype != 'datetime64[ns]':
                    df_pandas[self.incremental_column] = pd.to_datetime(df_pandas[self.incremental_column])

                # Filtrado directo en Pandas
                df_pandas = df_pandas[
                    df_pandas[self.incremental_column] > self.last_load_timestamp
                ]

            # 2. Iterar sobre las filas filtradas y convertirlas a filas de Spark
            # df_pandas.itertuples es más rápido que iterrows
            for row_data in df_pandas.itertuples(index=False):
                # Conversión directa a Row, confiando en que Pandas/PyQvd hizo la conversión de tipos
                spark_row = Row(*self.schema.names)(*row_data)
                yield spark_row

        except Exception as e:
            print(f"Error al leer QVD con PyQvd {file_path}: {e}")
            raise e

    def supports_reading_data_in_arrow_format(self) -> bool:
        """Habilitar la optimización Arrow."""
        return self.enable_arrow

class QvdDataSource(DataSource):
    """Punto de entrada para el conector 'qvd'."""
    @classmethod
    def name(cls):
        return "qvd"

    def schema(self):
        """Infieren el esquema leyendo la metadata del encabezado QVD."""
        file_path_pattern = self.options.get("path")
        file_paths = glob.glob(file_path_pattern)
        if not file_paths:
            raise FileNotFoundError(f"No se encontraron archivos QVD en la ruta: {file_path_pattern}")

        first_qvd_path = file_paths[0]

        try:
            # Inferir el esquema: Usamos chunk_size=1 para solo cargar la metadata del encabezado.
            temp_table = next(QvdTable.from_qvd(first_qvd_path, chunk_size=1))
            schema = map_qvd_metadata_to_spark_schema(temp_table)
        except Exception as e:
             raise RuntimeError(f"Error al parsear metadata de QVD en {first_qvd_path}: {e}")

        return schema

    def reader(self, schema: StructType):
        return QvdDataSourceReader(schema, self.options)
