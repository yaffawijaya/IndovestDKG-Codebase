import os
import time
import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# TAG baru
# TAG ="https://www.cnbcindonesia.com/tag/investasi?kanal=mymoney&page=2"
TAG = "https://www.cnbcindonesia.com/market/indeks/5?tipe=artikel&page=2"

START_PAGE = 2
END_PAGE = 4

# Gunakan absolute path dengan raw string untuk menangani backslashes di Windows
FILE_NAME = rf"Y:\Developer\projects\IndovestDKG\KG_CONSTRUCTION\scrap\result\cnbc\testing_cnbc_investment_{START_PAGE}_to_{END_PAGE}_pages.csv"

def setup_logging():
    logging.basicConfig(
        format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO
    )

class CNBCNewsExtractor:
    def __init__(self, base_url, max_retries=3, timeout=10):
        self.base_url = base_url
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()

    def collect_links(self, start_page, end_page):
        links = []
        # Parsing base_url untuk menangani query parameters dengan benar
        parsed_url = urlparse(self.base_url)
        query_params = parse_qs(parsed_url.query)
        # Hapus parameter 'page' yang sudah ada (jika ada)
        query_params.pop('page', None)
        
        for page in range(start_page, end_page + 1):
            # Tambahkan parameter page yang baru
            query_params['page'] = page
            new_query = urlencode(query_params, doseq=True)
            url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, 
                              parsed_url.params, new_query, parsed_url.fragment))
            logging.info(f"Collecting links from: {url}")
            for attempt in range(self.max_retries):
                try:
                    resp = self.session.get(url, timeout=self.timeout)
                    if resp.status_code == 200:
                        soup = BeautifulSoup(resp.text, "html.parser")
                        # Kumpulkan link dari setiap artikel berdasarkan struktur HTML CNBC
                        articles = soup.find_all("article")
                        for art in articles:
                            # Cari tag <a> dengan class spesifik (sesuai contoh HTML CNBC)
                            a_tag = art.find("a", class_="group flex gap-4 items-center")
                            if a_tag:
                                link = a_tag.get("href")
                                if link and link.startswith("https://www.cnbcindonesia.com/"):
                                    links.append(link)
                        break  # Keluar dari loop retry jika berhasil
                    else:
                        logging.error(f"Status code {resp.status_code} untuk {url}")
                except Exception as e:
                    logging.error(f"Error fetching {url}: {e}")
                time.sleep(1)
        return links

    def get_news_data(self, link):
        logging.info(f"Fetching article: {link}")
        for attempt in range(self.max_retries):
            try:
                resp = self.session.get(link, timeout=self.timeout)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, "html.parser")
                    # Ambil judul dari tag <h1> dengan class khusus
                    title_tag = soup.find("h1", class_="mb-4 text-32 font-extrabold")
                    title = title_tag.get_text(strip=True) if title_tag else ""
                    # Ambil tanggal dari <div> dengan class "text-cm text-gray"
                    date_tag = soup.find("div", class_="text-cm text-gray")
                    date = date_tag.get_text(strip=True) if date_tag else ""
                    # Ambil isi artikel dari <div> dengan class "detail-text"
                    content_div = soup.find("div", class_="detail-text")
                    if content_div:
                        paragraphs = content_div.find_all("p")
                        content = "\n".join([p.get_text(strip=True) for p in paragraphs])
                    else:
                        content = ""
                    return {"tanggal": date, "judul": title, "link": link, "isi": content}
                else:
                    logging.error(f"Status code {resp.status_code} untuk artikel {link}")
            except Exception as e:
                logging.error(f"Error fetching article {link}: {e}")
            time.sleep(1)
        return {"tanggal": "", "judul": "", "link": link, "isi": ""}

def write_to_csv(file_path, news_list):
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    if os.path.exists(file_path):
        df = pd.read_csv(file_path, sep=";")
    else:
        df = pd.DataFrame(columns=["tanggal", "judul", "link", "isi"])
    existing_links = set(df["link"].values)
    for news in news_list:
        if news["link"] in existing_links:
            df.loc[df["link"] == news["link"], ["tanggal", "judul", "isi"]] = [
                news["tanggal"], news["judul"], news["isi"]
            ]
        else:
            df = pd.concat([df, pd.DataFrame([news])], ignore_index=True)
            existing_links.add(news["link"])
    df.to_csv(file_path, sep=";", index=False)
    return file_path  # Return file path for confirmation

def main(start_page, end_page):
    setup_logging()
    start_time = time.time()
    # Inisialisasi extractor dengan TAG baru sebagai base URL
    extractor = CNBCNewsExtractor(TAG)
    csv_path = FILE_NAME
    links = extractor.collect_links(start_page, end_page)
    total_links = len(links)
    logging.info(f"Total links collected: {total_links}")
    if total_links == 0:
        logging.error("No links collected. Exiting.")
        return
    with ThreadPoolExecutor(max_workers=8) as executor:
        news_list = list(executor.map(extractor.get_news_data, links))
    result_path = write_to_csv(csv_path, news_list)
    logging.info(f"Data saved to {os.path.abspath(result_path)}")
    end_time = time.time()
    logging.info(f"Execution Time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main(START_PAGE, END_PAGE)
