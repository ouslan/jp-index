from .data_pull import DataPull
import polars as pl
import os

class DataProcess(DataPull):
    def __init__(self, debug:bool=False, data_dir:str='data'):
        self.debug = debug
        self.data_dir = data_dir
        super().__init__(debug)
        if not os.path.exists(f'{data_dir}/raw'):
            os.makedirs(f'{data_dir}/raw')
        if not os.path.exists(f'{data_dir}/processed'):
            os.makedirs(f'{data_dir}/processed')

    def process_consumer(self, file_name: str) -> None:
        self.pull_consumer(f"{self.data_dir}/raw/{file_name}")
        df = pl.read_excel(f"{self.data_dir}/raw/{file_name}", sheet_id=1)
        names = df.head(1).to_dicts().pop()
        names = dict((k, v.lower().replace(' ', '_').replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u').replace('ñ', 'n')) for k, v in names.items())
        df = df.rename(names)
        df = df.tail(-2).head(-1)
        df = df.with_columns(pl.col('descripcion').str.to_lowercase())
        df = df.with_columns((
            pl.when(pl.col('descripcion').str.contains("ene")).then(pl.col('descripcion').str.replace("ene", "01").str.split_exact('-', 1).struct.rename_fields(['month', 'year']).alias('date'))
              .when(pl.col('descripcion').str.contains("feb")).then(pl.col('descripcion').str.replace("feb", "02").str.split_exact('-', 1).struct.rename_fields(['month', 'year']).alias('date'))
              .when(pl.col('descripcion').str.contains("mar")).then(pl.col('descripcion').str.replace("mar", "03").str.split_exact('-', 1).struct.rename_fields(['month', 'year']).alias('date'))
              .when(pl.col('descripcion').str.contains("abr")).then(pl.col('descripcion').str.replace("abr", "04").str.split_exact('-', 1).struct.rename_fields(['month', 'year']).alias('date'))
              .when(pl.col('descripcion').str.contains("may")).then(pl.col('descripcion').str.replace("may", "05").str.split_exact('-', 1).struct.rename_fields(['month', 'year']).alias('date'))
              .when(pl.col('descripcion').str.contains("jun")).then(pl.col('descripcion').str.replace("jun", "06").str.split_exact('-', 1).struct.rename_fields(['month', 'year']).alias('date'))
              .when(pl.col('descripcion').str.contains("jul")).then(pl.col('descripcion').str.replace("jul", "07").str.split_exact('-', 1).struct.rename_fields(['month', 'year']).alias('date'))
              .when(pl.col('descripcion').str.contains("ago")).then(pl.col('descripcion').str.replace("ago", "08").str.split_exact('-', 1).struct.rename_fields(['month', 'year']).alias('date'))
              .when(pl.col('descripcion').str.contains("sep")).then(pl.col('descripcion').str.replace("sep", "09").str.split_exact('-', 1).struct.rename_fields(['month', 'year']).alias('date'))
              .when(pl.col('descripcion').str.contains("oct")).then(pl.col('descripcion').str.replace("oct", "10").str.split_exact('-', 1).struct.rename_fields(['month', 'year']).alias('date'))
              .when(pl.col('descripcion').str.contains("nov")).then(pl.col('descripcion').str.replace("nov", "11").str.split_exact('-', 1).struct.rename_fields(['month', 'year']).alias('date'))
              .when(pl.col('descripcion').str.contains("dic")).then(pl.col('descripcion').str.replace("dic", "12").str.split_exact('-', 1).struct.rename_fields(['month', 'year']).alias('date'))
              .otherwise(pl.col('descripcion').str.split_exact('-', 1).struct.rename_fields(['year', 'month']).alias('date'))
          )).unnest("date")
        df = df.with_columns((
          pl.when((pl.col("year").str.len_chars() == 2) & (pl.col("year").str.strip_chars().cast(pl.Int32) < 80)).then(pl.col("year").str.strip_chars().cast(pl.Int32) + 2000)
            .when((pl.col("year").str.len_chars() == 2) & (pl.col("year").str.strip_chars().cast(pl.Int32) >= 80)).then(pl.col("year").str.strip_chars().cast(pl.Int32) + 1900)
            .otherwise(pl.col("year").str.strip_chars().cast(pl.Int32)).alias("year")
        ))
        df = df.with_columns(date=pl.date(pl.col('year').cast(pl.String), pl.col('month'), 1))
        df = df.drop(['year', 'month', 'descripcion'])
        df = df.with_columns(pl.all().exclude("date").cast(pl.Float64))
        df.write_parquet(f"{self.data_dir}/processed/{file_name.replace('.xls', '.parquet')}")



