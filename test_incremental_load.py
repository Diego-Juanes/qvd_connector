from pyspark.sql.session import SparkSession
from qvd_connector import QvdDataSource

if __name__ == '__main__':
    # 1. Inicializar Spark
    spark = SparkSession.builder \
        .appName("QVDIncrementalTest") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

    # 2. Registrar la Fuente de Datos
    spark.dataSource.register(QvdDataSource)

    # 3. Configuración para la Carga Incremental
    # Este valor debe estar en formato YYYY-MM-DD HH:MM:SS
    LAST_LOAD_TS = "2025-01-01 00:00:00"

    print("==========================================================")
    print("  PRUEBA DE CARGA INCREMENTAL DEL CONECTOR QVD")
    print(f"  Filtro: Fecha_Actualizacion > {LAST_LOAD_TS}")
    print("==========================================================")

    # 4. Ejecutar la carga incremental
    df_incremental = spark.read \
        .format("qvd") \
        .option("path", "transacciones_*.qvd") \
        .option("incrementalColumn", "Fecha_Actualizacion") \
        .option("lastLoadTimestamp", LAST_LOAD_TS) \
        .option("enableArrow", "true") \
        .load()

    print("\nEsquema de datos inferido:")
    df_incremental.printSchema()

    # Contar las filas para verificar el filtro
    count = df_incremental.count()
    print(f"\nResultados (Filas cargadas: {count}. Esperadas: 3):")
    df_incremental.show(truncate=False)

    # 5. Detener la sesión
    spark.stop()
