import pandas as pd
from pyqvd import QvdTable
from datetime import datetime

# --- 1. Archivo QVD Antiguo (Para ser excluido por la carga incremental) ---
# Se utiliza una fecha pasada como punto de corte.
data_old = {
    'ID_Transaccion': [1001, 1002],
    'Monto': [50.00, 150.50],
    'Fecha_Actualizacion': [
        datetime(2024, 1, 15, 10, 0),
        datetime(2024, 2, 20, 15, 30)
    ]
}
df_old = pd.DataFrame(data_old)
qvd_table_old = QvdTable.from_pandas(df_old)
qvd_table_old.to_qvd('data/transacciones_2024.qvd')
print("Creado: data/transacciones_2024.qvd")

# --- 2. Archivo QVD Nuevo (Para ser incluido por la carga incremental) ---
# Se utiliza una fecha futura (o posterior al punto de corte) como marca.
data_new = {
    'ID_Transaccion': [2001, 2002, 2003],
    'Monto': [500.25, 10.00, 300.75],
    'Fecha_Actualizacion': [
        datetime(2025, 1, 15, 8, 0), # ⬅️ Este es nuestro punto de corte
        datetime(2025, 2, 1, 12, 0),
        datetime(2025, 2, 10, 9, 0)
    ]
}
df_new = pd.DataFrame(data_new)
qvd_table_new = QvdTable.from_pandas(df_new)
qvd_table_new.to_qvd('data/transacciones_2025.qvd')
print("Creado: data/transacciones_2025.qvd")
