from tqdm import tqdm
import requests
import os


class DataPull:
    def __init__(self, debug:bool=False):
        self.debug = debug

    def pull_file(self, url:str, filename:str, verify:bool=True) -> None:
        if os.path.exists(filename):
            if self.debug:
                print("\033[0;36mNOTICE: \033[0m" + f"File {filename} already exists, skipping download")
        else:
            chunk_size = 10 * 1024 * 1024

            with requests.get(url, stream=True, verify=verify) as response:
                total_size = int(response.headers.get('content-length', 0))

                with tqdm(total=total_size, unit='B', unit_scale=True, unit_divisor=1024, desc='Downloading') as bar:
                    with open(filename, 'wb') as file:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            if chunk:
                                file.write(chunk)
                                bar.update(len(chunk))  # Update the progress bar with the size of the chunk
            if self.debug:
                print("\033[0;32mSUCCESS: \033[0m" + f"Downloaded {filename}")

    def pull_economic_indicators(self, file_path: str):
        url = "https://jp.pr.gov/wp-content/uploads/2024/09/Indicadores_Economicos_9.13.2024.xlsx"
        self.pull_file(url, file_path)

    def pull_consumer(self, file_path: str):
        data = {
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
            '__VIEWSTATE': '/wEPDwUKLTcxODY5NDM5NGQYAQUeX19Db250cm9sc1JlcXVpcmVQb3N0QmFja0tleV9fFgQFEmN0bDAwJEltYWdlQnV0dG9uMgUSY3RsMDAkSW1hZ2VCdXR0b243BRJjdGwwMCRJbWFnZUJ1dHRvbjYFE2N0bDAwJEltYWdlQnV0dG9uMjmbsvgxbeWK6OTKG+ORpFm3OEWkw9qqUvxB+x/6e4VYHA==',
            '__VIEWSTATEGENERATOR': 'C7F80305',
            '__PREVIOUSPAGE': 'GwKz4yZIj7Uo4a3KIzGOhMCI77vcHU60FinRHRx3g5l-ETFOoBNr2no6Fz0HhRbD6EHh7WoeeB373TP67fTBUnA29kstVUAarj4oAEiJ6fSKHGteKw7kiIEmcASoaXYmTyut_TAQm5UxmEzDwNnRMA2',
            '__EVENTVALIDATION': '/wEdAAlUqk7OF8+IyJVVhTuf5Y1+K54MsQ9Z5Tipa4C3CU9lIy6KqsTtzWiK229TcIgvoTmJ5D8KsXArXsSdeMqOt6pk+d3fBy3LDDz0lsNt4u+CuDIENRTx3TqpeEC0BFNcbx18XLv2PDpbcvrQF1sPng9RHC+hNwNMKsAjTYpq3ZLON4FBZYDVNXrnB/9WmjDFKj6ji8qalfcp0F7IzcRWfkdgwm54EtTOkeRtMO19pSuuIg==',
            'ctl00$MainContent$Button1': 'Descargar',
        }

        # Perform the POST request to download the file
        response = requests.post(
            'https://www.mercadolaboral.pr.gov/Tablas_Estadisticas/Otras_Tablas/T_Indice_Precio.aspx',
            data=data,
            stream=True  # Stream the response to handle large files
        )

        # Check if the request was successful
        if response.status_code == 200:
            # Get the total file size from the headers
            total_size = int(response.headers.get('content-length', 0))
            # Open the file for writing in binary mode
            with open(file_path, 'wb') as file:
                # Use tqdm to show the download progress
                for chunk in tqdm(response.iter_content(chunk_size=8192), 
                                  total=total_size // 8192, 
                                  unit='KB', 
                                  desc='Downloading'):
                    if chunk:  # Filter out keep-alive new chunks
                        file.write(chunk)
            if self.debug:
                print(f"\033[0;32mSUCCESS: \033[0mDownloaded file to {file_path}")
        else:
            if self.debug:
                print(f"\033[0;31mERROR: \033[0mFailed to download file. Status code: {response.status_code}")


