from .data_pull import DataPull
from datetime import datetime
import polars.selectors as cs
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
        if not os.path.exists(f"{self.data_dir}/raw/{file_name}"):
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

    def process_jp_index(self, file_name: str) -> pl.DataFrame:

        if not os.path.exists(f"{self.data_dir}/raw/{file_name}"):
            self.pull_economic_indicators(f"{self.data_dir}/raw/{file_name}")

        jp_df = self.process_sheet(f"{self.data_dir}/raw/{file_name}", 3)

        for sheet in range(4, 20):
            df = self.process_sheet(f"{self.data_dir}/raw/{file_name}", sheet)
            jp_df = jp_df.join(df, on=['date'], how='left', validate="1:1")

        return jp_df


    def process_sheet(self, file_path : str, sheet_id: int) -> pl.DataFrame:
        df = pl.read_excel(file_path, sheet_id=sheet_id)
        months = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre", "Meses"]
        col_name = df.columns[1].strip().replace(' ', '_').lower().replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u').replace('ñ', 'n')

        df = df.filter(pl.nth(1).is_in(months)).drop(cs.first()).head(13)
        columns = df.head(1).with_columns(pl.all()).cast(pl.String).to_dicts().pop()
        for item in columns:
          if columns[item] == "Meses":
            continue
          elif columns[item] is None:
            df = df.drop(item)
          elif float(columns[item]) < 2000 or float(columns[item]) > datetime.now().year + 1:
            df = df.drop(item)

        if len(df.columns) > (datetime.now().year - 1997):
            df = df.select(pl.nth(range(0, len(df.columns)//2)))

        df = df.rename(df.head(1).with_columns(pl.nth(range(1, len(df.columns))).cast(pl.Int64)).cast(pl.String).to_dicts().pop()).tail(-1)
        df = df.with_columns(pl.col("Meses").str.to_lowercase()).cast(pl.String)
        df = self.process_panel(df, col_name)

        return df

    def process_panel(self, df: pl.DataFrame, col_name:str) -> pl.DataFrame:
        empty_df = [
            pl.Series("date", [], dtype=pl.Datetime),
            pl.Series(col_name, [], dtype=pl.Float64)
        ]
        clean_df = pl.DataFrame(empty_df)

        for column in df.columns:
                    if column == "Meses":
                        continue
                    column_name = col_name
                    # Create a temporary DataFrame
                    tmp = df
                    tmp = tmp.rename({column:column_name})
                    tmp = tmp.with_columns(
                    pl.when(pl.col("Meses").str.strip_chars().str.to_lowercase() == "enero").then(1)
                      .when(pl.col("Meses").str.strip_chars().str.to_lowercase() == "febrero").then(2)
                      .when(pl.col("Meses").str.strip_chars().str.to_lowercase() == "marzo").then(3)
                      .when(pl.col("Meses").str.strip_chars().str.to_lowercase() == "abril").then(4)
                      .when(pl.col("Meses").str.strip_chars().str.to_lowercase() == "mayo").then(5)
                      .when(pl.col("Meses").str.strip_chars().str.to_lowercase() == "junio").then(6)
                      .when(pl.col("Meses").str.strip_chars().str.to_lowercase() == "julio").then(7)
                      .when(pl.col("Meses").str.strip_chars().str.to_lowercase() == "agosto").then(8)
                      .when(pl.col("Meses").str.strip_chars().str.to_lowercase() == "septiembre").then(9)
                      .when(pl.col("Meses").str.strip_chars().str.to_lowercase() == "octubre").then(10)
                      .when(pl.col("Meses").str.strip_chars().str.to_lowercase() == "noviembre").then(11)
                      .when(pl.col("Meses").str.strip_chars().str.to_lowercase() == "diciembre").then(12)
                      .alias("month")
                        )
                    tmp = tmp.with_columns(
                        pl.col(column_name).str.replace_all("$", "", literal=True).str.replace_all(",", "").str.strip_chars().alias(column_name)
                    )
                    tmp = tmp.with_columns(
                        pl.when(pl.col(column_name)  == "n/d").then(None)
                        .when(pl.col(column_name)  == "**").then(None)
                        .when(pl.col(column_name)  == "-").then(None)
                        .when(pl.col(column_name)  == "no disponible").then(None)
                        .otherwise(pl.col(column_name)).alias(column_name)
                    )
                    tmp = tmp.select(
                                    pl.col("month").cast(pl.Int64).alias("month"),
                                    pl.lit(int(column)).cast(pl.Int64).alias("year"),
                                    pl.col(column_name).cast(pl.Float64).alias(column_name)
                    )

                    tmp = tmp.with_columns((pl.col("year").cast(pl.String) + "-" + pl.col("month").cast(pl.String) + "-01").alias("date"))
                    tmp = tmp.select(pl.col("date").str.to_datetime("%Y-%m-%d").alias("date"),
                                     pl.col(column_name).alias(column_name))

                    # Append the temporary DataFrame to the list
                    clean_df = pl.concat([clean_df, tmp], how="vertical")
        return clean_df
