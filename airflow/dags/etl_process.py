import datetime

from airflow.decorators import dag, task

markdown_text = """
### ETL para datos de Empresa Línea 216 S.A.T. en base a información de ACTrans

Este DAG extrae la información de un archivo Excel en Google Drive donde cada hoja
contiene los datos entre 2020 y 2025 de cada línea de la Empresa Línea 216 S.A.T.

Procesa los datos imputando valores nulos, eliminando outliers por la mediana, y guardando
el preprocesamiento en archivos CSV (uno por cada línea) en un bucket de S3

Luego separa cada CSV en entrenamiento y test, en proporciones 80/20 teniendo en cuenta
que al ser series temporales la separación debe ser partir de una fecha de manera ordenada.

Además, solo se tienen en cuenta los datos a partir del 01/01/2022 para excluir datos atípicos
ocurridos durante la pandemia de COVID-19.
"""

default_args = {
    'owner': "Bernardo Maximiliano José, Tacchella Alejandro Nicolás",
    'depends_on_past': False,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'dagrun_timeout': datetime.timedelta(minutes=15)
}

@dag(
    dag_id="process_etl_actrans_data",
    description="",
    doc_md=markdown_text,
    tags=["ETL", "Empresa Línea 216 S.A.T. (ACTrans Data)"],
    default_args=default_args,
    catchup=False,
)
def process_etl_actrans_data():

    @task.virtualenv(
        task_id="obtain_original_data",
        requirements=[
            "awswrangler==3.6.0",
            "gdown==5.2.0",
        ],
        system_site_packages=True
    )
    def get_dataset():
        import awswrangler as wr
        import gdown

        file_id = "19BJlSiSR8V5mgPAuoLKfqSpl2cnGLbUD"
        url = f"https://drive.google.com/uc?id={file_id}"
        output = "trx_recaudacion_km_empresa_54_2020_2025.xlsx"

        gdown.download(url, output, quiet=False)

        s3_path = f"s3://data/raw/{output}"

        wr.s3.upload(local_file=output, path=s3_path)


    @task.virtualenv(
        task_id="process_dataset",
        requirements=["awswrangler[openpyxl]==3.6.0"],
        system_site_packages=True
    )
    def process_dataset():
        import awswrangler as wr
        import pandas as pd

        data_original_path = "s3://data/raw/trx_recaudacion_km_empresa_54_2020_2025.xlsx"
        data_end_path = "s3://data/processed/{}/data.csv"
        sheets = wr.s3.read_excel(data_original_path, sheet_name=None)

        lineas_dfs = {}

        for nombre_hoja, df in sheets.items():
            lineas_dfs[nombre_hoja] = df

        for linea, df in lineas_dfs.items():
            df['FECHA'] = pd.to_datetime(df['FECHA'])
            # Completamos valores nulos en CANT. TRX, y KM
            df['CANT. TRX'] = df['CANT. TRX'].ffill()
            df['KM'] = df['KM'].ffill()
            # Borramos columna de RECAUDACIÓN porque no está ajustada
            # por inflación y no es comparable
            df.drop(columns=['RECAUDACION'], inplace=True, errors='ignore')
            # Reordenar columnas
            columnas = ['FECHA', 'CANT. TRX', 'KM']
            df = df[columnas]
            df.rename(columns={'FECHA': 'ds', 'CANT. TRX': 'y', 'KM': 'km'}, inplace=True)
            lineas_dfs[linea] = df


        def reemplazar_outliers_por_mediana(df, columnas):
            df = df.copy()
            for col in columnas:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                mediana = df[col].quantile(0.5)
                IQR = Q3 - Q1
                limite_inferior = Q1 - 1.5 * IQR
                limite_superior = Q3 + 1.5 * IQR
                mask_outlier = (df[col] < limite_inferior) | (df[col] > limite_superior)
                df.loc[mask_outlier, col] = mediana
            return df

        columnas_a_filtrar = ['y', 'km']

        lineas_dfs = {
            linea: reemplazar_outliers_por_mediana(df, columnas_a_filtrar)
            for linea, df in lineas_dfs.items()
        }

        for linea, df in lineas_dfs.items():
            wr.s3.to_csv(df=df, path=data_end_path.format(linea), index=False)

    @task.virtualenv(
        task_id="split_dataset",
        requirements=["awswrangler[openpyxl]==3.6.0",],
        system_site_packages=True
    )
    def split_dataset():
        from airflow.models import Variable
        import awswrangler as wr
        from datetime import datetime
        import pandas as pd

        raw_data_path = "s3://data/raw/trx_recaudacion_km_empresa_54_2020_2025.xlsx"
        processed_data_path = "s3://data/processed/{}/data.csv"
        train_data_path = "s3://data/processed/{}/train.csv"
        test_data_path = "s3://data/processed/{}/test.csv"

        start_date = datetime.strptime(
            Variable.get("actrans_start_date"),
            Variable.get("actrans_start_date_format")
        )

        sheets = wr.s3.read_excel(raw_data_path, sheet_name=None)
        lineas = sheets.keys()

        for linea in lineas:
            # Ordenar por fecha
            df = wr.s3.read_csv(processed_data_path.format(linea))
            df['ds'] = pd.to_datetime(df['ds'])
            df = df.set_index('ds', drop=False)  # establecer ds como índice
            df = df[df.index >= start_date].sort_index()

            # Obtener fecha de corte (80%)
            # Convertir fechas a enteros para usar quantile, luego volver a datetime
            fechas_numeric = df.index.view('int64')
            fecha_corte_ts = pd.Series(fechas_numeric).quantile(0.8)
            fecha_corte = pd.to_datetime(fecha_corte_ts)

            # Dividir el DataFrame
            df_train = df[df.index <= fecha_corte]  # 80%
            df_test = df[df.index > fecha_corte]   # 20%
            wr.s3.to_csv(df=df_train, path=train_data_path.format(linea), index=False)
            wr.s3.to_csv(df=df_test, path=test_data_path.format(linea), index=False)


    get_data() >> process_data() >> split_dataset()

etl_actrans_data_dag = process_etl_actrans_data()
