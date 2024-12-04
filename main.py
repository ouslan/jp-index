from src.data.data_process import DataProcess

def main() -> None:
    d = DataProcess("sqlite:///db.sqlite", "data/")
    print(d.consumer_data().to_pandas())
    #print(d.process_jp_index().to_pandas())

if __name__ == "__main__":
    main()
