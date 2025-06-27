import pandas as pd
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from modules.redundant_cleaning import clean_and_convert_date, remove_title_from_isi, remove_redundancy
from modules.news_extractor import NewsExtractor

START_PAGE = 3
END_PAGE = 5
FILE_NAME = f"testing_{START_PAGE}_to_{END_PAGE}_pages.csv"

def setup_logging():
    logging.basicConfig(format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S", level=logging.INFO)

def read_existing_csv(file_path):
    if os.path.exists(file_path):
        df = pd.read_csv(file_path, sep=";")
        return df.set_index("link").to_dict(orient="index")
    return {}

def write_to_csv(file_path, news_list):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
    if os.path.exists(file_path):
        df = pd.read_csv(file_path, sep=";")
    else:
        df = pd.DataFrame(columns=["tanggal", "judul", "link", "isi"])
    existing_links = set(df["link"].values)
    for news in news_list:
        if news["link"] in existing_links:
            df.loc[df["link"] == news["link"], ["tanggal", "judul", "isi"]] = [news["tanggal"], news["judul"], news["isi"]]
        else:
            df = pd.concat([df, pd.DataFrame([news])], ignore_index=True)
            existing_links.add(news["link"])
    df.to_csv(file_path, sep=";", index=False)

def main(start_page, end_page):
    setup_logging()
    start_time = time.time()
    extractor = NewsExtractor("https://www.kompas.com/tag/investasi", max_retries=3, timeout=10)
    csv_path = os.path.join("result", FILE_NAME)
    links = extractor.collect_links(start_page, end_page)
    total_links = len(links)
    if total_links == 0:
        logging.error("No links collected. Exiting.")
        return
    with ThreadPoolExecutor(max_workers=16) as executor:
        news_list = list(executor.map(lambda t: extractor.get_news_data(t[1], t[0], total_links),
                                      enumerate(links, start=1)))
    write_to_csv(csv_path, news_list)
    df = pd.read_csv(csv_path, sep=";")
    df['tanggal'] = df['tanggal'].apply(clean_and_convert_date)
    df['isi'] = df.apply(remove_title_from_isi, axis=1)
    df['isi'] = df['isi'].apply(remove_redundancy)
    df.to_csv(csv_path, sep=";", index=False)
    extractor.close()
    end_time = time.time()
    logging.info(f"Execution Time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main(START_PAGE, END_PAGE)
