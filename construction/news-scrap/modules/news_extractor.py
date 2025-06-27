import httpx
import logging
from selectolax.parser import HTMLParser
from urllib.parse import urljoin
import time
import random

class NewsExtractor:
    def __init__(self, base_url, max_retries=3, timeout=10):
        self.base_url = base_url
        self.max_retries = max_retries
        self.timeout = timeout
        self.client = httpx.Client(timeout=timeout)
        self.cache = {}

    def get_html(self, url):
        if url in self.cache:
            return self.cache[url]
        # Rotasi user-agent dan delay acak untuk mengurangi kemungkinan blokir
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"
        ]
        headers = {
            "User-Agent": random.choice(user_agents),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        }
        time.sleep(random.uniform(1, 3))
        for attempt in range(self.max_retries):
            try:
                resp = self.client.get(url, headers=headers)
                resp.raise_for_status()
                parser = HTMLParser(resp.text)
                self.cache[url] = parser
                return parser
            except (httpx.HTTPStatusError, httpx.RequestError) as e:
                logging.warning(f"Error on {url}: {e}. Attempt {attempt+1}/{self.max_retries}")
                time.sleep(2 * (attempt + 1))
        self.cache[url] = None
        return None

    def get_news_data(self, link, article_number, total_articles):
        logging.info(f"Processing article {article_number} of {total_articles}: {link}")
        html = self.get_html(link)
        if html:
            tanggal = html.css_first("div.read__time").text() if html.css_first("div.read__time") else "N/A"
            judul = html.css_first("h1.read__title").text() if html.css_first("h1.read__title") else "N/A"
            paragraphs = html.css("div.read__content p")
            isi = " ".join(p.text() for p in paragraphs) if paragraphs else "N/A"
            return {"tanggal": tanggal, "judul": judul, "link": link, "isi": isi}
        logging.info(f"Failed processing article {article_number} of {total_articles}: {link}")
        return {"tanggal": "error", "judul": "error", "link": link, "isi": "error"}

    def collect_links(self, start_page, end_page):
        all_links = []
        for page in range(start_page, end_page + 1):
            url = urljoin(self.base_url, f"?sort=asc&page={page}")
            html = self.get_html(url)
            if html:
                items = html.css("div.articleList.-list div.articleItem")
                for item in items:
                    a_tag = item.css_first("a.article-link")
                    if a_tag:
                        link = a_tag.attributes.get("href", "")
                        if link:
                            all_links.append(link.replace("http://", "https://"))
                logging.info(f"Processed page {page} ({start_page} to {end_page}) with {len(items)} articles.")
            else:
                logging.warning(f"Failed to retrieve content on page {page}.")
        return all_links

    def close(self):
        self.client.close()