import os

import sys

import pandas as pd
from sqlmodel import create_engine, Session, SQLModel
from tqdm import tqdm

# Añadir el directorio raíz del proyecto al sys.path
sys.path.append(os.path.abspath("/home/jjrm/jp-index-2/src"))

from dao.awards_table import AwardTable


class AwardDataCleanerInserter:
    def __init__(
        self,
        data_dir: str = "data",
        database_url: str = "sqlite:///db.sqlite",
        debug: bool = False,
    ):
        self.data_dir = data_dir
        self.database_url = database_url
        self.engine = create_engine(database_url)
        self.debug = debug
        self._prepare_directories()
        self._create_table()

    def _prepare_directories(self):
        os.makedirs(f"{self.data_dir}/processed", exist_ok=True)

    def _create_table(self):
        SQLModel.metadata.create_all(self.engine)

    def process_and_insert(self):
        raw_files = [
            f"{self.data_dir}/raw/{file}"
            for file in os.listdir(f"{self.data_dir}/raw")
            if file.endswith(".csv")
        ]

        for raw_file in raw_files:
            try:
                print(f"\033[0;34mProcessing: {raw_file}\033[0m")
                cleaned_df = self.clean_data(raw_file)

                # Ensure `cleaned_df` is a DataFrame
                if isinstance(cleaned_df, pd.Series):
                    cleaned_df = cleaned_df.to_frame()

                # Save cleaned data to a processed file
                processed_file = raw_file.replace("/raw/", "/processed/")
                cleaned_df.to_csv(processed_file, index=False)

                # Insert cleaned data into the database
                self.insert_into_db(cleaned_df)
                print(
                    f"\033[0;32mSUCCESS: \033[0mData from {raw_file} inserted into the database"
                )
            except Exception as e:
                print(
                    f"\033[0;31mERROR: \033[0mFailed to process {raw_file}. Reason: {e}"
                )

    def clean_data(self, file_path: str) -> pd.DataFrame:
        """
        Cleans the raw awards data CSV file.

        Args:
            file_path (str): Path to the raw CSV file.

        Returns:
            pd.DataFrame: Cleaned DataFrame ready for insertion into the database.
        """
        try:
            # Read the CSV file into a DataFrame
            df = pd.read_csv(file_path)

            # Perform any necessary cleaning steps here
            # Example: Rename columns to match database schema
            df = df.rename(
                columns={
                    "Award ID": "award_id",
                    "Recipient Name": "recipient_name",
                    "Start Date": "start_date",
                    "End Date": "end_date",
                    "Award Amount": "award_amount",
                    "Awarding Agency": "awarding_agency",
                    "Awarding Sub Agency": "awarding_sub_agency",
                    "Funding Agency": "funding_agency",
                    "Funding Sub Agency": "funding_sub_agency",
                    "Award Type": "award_type",
                }
            )

            # Optional: Drop rows with missing critical data
            df = df.dropna(subset=["award_id", "award_amount"])

            # Convert date columns to proper datetime format
            df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
            df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")

            # Fill or clean up other columns as needed
            df["award_amount"] = df["award_amount"].fillna(0)

            return df
        except Exception as e:
            print(f"\033[0;31mERROR: \033[0mFailed to clean {file_path}. Reason: {e}")
            raise

    def insert_into_db(self, cleaned_df: pd.DataFrame):
        """
        Inserts the cleaned DataFrame into the database.

        Args:
            cleaned_df (pd.DataFrame): Cleaned DataFrame.
        """
        try:
            with Session(self.engine) as session:
                for _, row in tqdm(
                    cleaned_df.iterrows(),
                    total=cleaned_df.shape[0],
                    desc="Inserting into DB",
                ):
                    award = AwardTable(**row.to_dict())
                    session.add(award)
                session.commit()
        except Exception as e:
            print(
                f"\033[0;31mERROR: \033[0mFailed to insert data into the database. Reason: {e}"
            )
            raise
