#-------------------------------------------------------------------------
# Archivo para pruebas de código
#-------------------------------------------------------------------------

from pathlib import Path
from config.paths import PATH_BRONZE_DELTALAKE_INCREMENTAL
import pandas as pd
from src.load.delta_writer import leer_delta_lake
import sys


# df = leer_delta_lake(PATH_BRONZE_DELTALAKE_INCREMENTAL)

# print(df.head())

# max_id = df['id'].max()
# min_id = df['id'].min()
# print(min_id, max_id)


# df_total = leer_delta_lake(PATH_BRONZE_DELTALAKE_INCREMENTAL)
# print(df_total.to_string())
# print(df_total.shape[0])
print(Path(__file__))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
for path in sys.path:
    print(path)