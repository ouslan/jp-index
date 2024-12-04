from src.data.data_process import DataIndex

def main() -> None:
    d = DataIndex("sqlite:///db.sqlite", "data/")
    print(d.process_consumer().to_pandas())
    print(d.process_jp_index().to_pandas())

if __name__ == "__main__":
    main()
