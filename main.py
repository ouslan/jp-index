from src.data.data_process import DataProcess

def main() -> None:
    d = DataProcess("sqlite:///db.sqlite", "data/")
    print(d.jp_index_data())

if __name__ == "__main__":
    main()
