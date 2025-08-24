import datetime

from airflow.decorators import dag, task

markdown_text = """
### ETL for Empresa Línea 216 S.A.T. data based on ACTrans information

This DAG extracts data from an Excel file on Google Drive where each sheet
contains the 2020-2025 data for each line of Empresa Línea 216 S.A.T.

It processes the data by imputing missing values, removing outliers using the median,
and saving the preprocessed data as CSV files (one per line) in an S3 bucket.

Then, it splits each CSV into training and test sets with an 80/20 ratio, taking into account
that for time series the split should be done chronologically starting from a specific date.

Additionally, only data from 01/01/2022 onwards is considered to exclude atypical data
that occurred during the COVID-19 pandemic.
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
    description="ETL DAG for ACTrans line data",
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

        original_data_path = "s3://data/raw/trx_recaudacion_km_empresa_54_2020_2025.xlsx"
        processed_data_path = "s3://data/processed/{}/data.csv"
        sheets = wr.s3.read_excel(original_data_path, sheet_name=None)

        line_dfs = {}

        for sheet_name, df in sheets.items():
            line_dfs[sheet_name] = df

        for line, df in line_dfs.items():
            df['FECHA'] = pd.to_datetime(df['FECHA'])
            # Fill missing values in TRX_COUNT and KM
            df['CANT. TRX'] = df['CANT. TRX'].ffill()
            df['KM'] = df['KM'].ffill()
            # Drop REVENUE column because it's not inflation-adjusted and not comparable
            df.drop(columns=['RECAUDACION'], inplace=True, errors='ignore')
            # Reorder columns
            columns = ['FECHA', 'CANT. TRX', 'KM']
            df = df[columns]
            df.rename(columns={'FECHA': 'ds', 'CANT. TRX': 'y', 'KM': 'km'}, inplace=True)
            line_dfs[line] = df


        def replace_outliers_with_median(df, columns):
            df = df.copy()
            for col in columns:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                median = df[col].quantile(0.5)
                IQR = Q3 - Q1
                lower_limit = Q1 - 1.5 * IQR
                upper_limit = Q3 + 1.5 * IQR
                mask_outlier = (df[col] < lower_limit) | (df[col] > upper_limit)
                df.loc[mask_outlier, col] = median
            return df

        columns_to_filter = ['y', 'km']

        line_dfs = {
            line: replace_outliers_with_median(df, columns_to_filter)
            for line, df in line_dfs.items()
        }

        for line, df in line_dfs.items():
            wr.s3.to_csv(df=df, path=processed_data_path.format(line), index=False)

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
        lines = sheets.keys()

        for line in lines:
            # Sort by date
            df = wr.s3.read_csv(processed_data_path.format(line))
            df['ds'] = pd.to_datetime(df['ds'])
            df = df.set_index('ds', drop=False)
            df = df[df.index >= start_date].sort_index()

            # Get cutoff date (80%)
            numeric_dates = df.index.view('int64')
            cutoff_ts = pd.Series(numeric_dates).quantile(0.8)
            cutoff_date = pd.to_datetime(cutoff_ts)

            # Split DataFrame
            df_train = df[df.index <= cutoff_date]  # 80%
            df_test = df[df.index > cutoff_date]   # 20%
            wr.s3.to_csv(df=df_train, path=train_data_path.format(line), index=False)
            wr.s3.to_csv(df=df_test, path=test_data_path.format(line), index=False)


    get_dataset() >> process_dataset() >> split_dataset()

etl_actrans_data_dag = process_etl_actrans_data()
