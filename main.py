from src.data.data_process import DataProcess

def main() -> None:
    d = DataProcess("sqlite:///db.sqlite", "data/")
    print(d.consumer_data())
    print(d.process_jp_index())

if __name__ == "__main__":
    main()
