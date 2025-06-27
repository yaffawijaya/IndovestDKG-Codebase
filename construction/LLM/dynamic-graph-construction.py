import nest_asyncio
nest_asyncio.apply()

from langchain.output_parsers.openai_tools import JsonOutputKeyToolsParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field
from typing import List
import pandas as pd
from dotenv import load_dotenv
import os
import asyncio
from tqdm import tqdm
import json
import random

load_dotenv()
openai_api_key = os.getenv('OPENAI_API_KEY')
os.environ["OPENAI_API_KEY"] = openai_api_key

# Load your dataset (df should already be defined)
# data = df.copy(deep=True)
# data = data.tail(10)
data = "ok!"

# Define the range to process. These values determine which rows to process.
START_ARTICLE_ROW = 0
END_ARTICLE_ROW = len(data)

# File paths
JSON_OUTPUT_PATH = r"Y:\Developer\projects\IndovestDKG\KG_CONSTRUCTION\data\JSON_GRAPH\10rev-IndovestDKompasNews.jsonl"
CHECKPOINT_FILE = r"Y:\Developer\projects\IndovestDKG\KG_CONSTRUCTION\data\JSON_GRAPH\extraction_checkpoint.json"

# Create output directory if it doesn't exist.
output_dir = os.path.dirname(JSON_OUTPUT_PATH)
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Load checkpoint if exists; otherwise, start from START_ARTICLE_ROW.
if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
        checkpoint = json.load(f)
    last_processed_index = checkpoint.get("last_processed_index", START_ARTICLE_ROW)
    print(f"Resuming extraction from global row {last_processed_index + 1}")
else:
    last_processed_index = START_ARTICLE_ROW

# Slice the DataFrame to process only the rows from the checkpoint to END_ARTICLE_ROW.
data_to_process = data.iloc[last_processed_index:END_ARTICLE_ROW].reset_index(drop=True)

# Define the extraction models.
class InvestmentNewsEntity(BaseModel):
    subject: str = Field(description="Entitas utama, contoh: 'Bank Indonesia'")
    subject_type: str = Field(description="Tipe entitas, pilih dari: ORGANISASI, PEMERINTAHAN, BADAN_REGULATOR, NEGARA, KOTA, WILAYAH, ORANG, PERUSAHAAN, PRODUK, EVENT, SEKTOR, INDIKATOR_EKONOMI, INSTRUMEN_FINANSIAL, KONSEP")
    relation: str = Field(description="Hubungan, pilih dari: Memiliki, Mengumumkan, BeroperasiDi, Memperkenalkan, Menghasilkan, Mengendalikan, Berpartisipasi, Mempengaruhi, BerdampakPositif, BerdampakNegatif, Mengaitkan, AnggotaDari, BerinvestasiDi, Meningkatkan, Menurunkan")
    object: str = Field(description="Entitas target")
    object_type: str = Field(description="Tipe entitas target")

class InvestmentNewsEntities(BaseModel):
    entities: List[InvestmentNewsEntity] = Field(description="Daftar hubungan entitas dari berita")

model = ChatOpenAI(model="gpt-4o-mini").bind_tools([InvestmentNewsEntities])

system_prompt = """
Anda adalah ahli ekstraksi entitas dari berita investasi Indonesia.
Tugas Anda adalah mengekstrak SEMUA hubungan entitas yang ada di dalam teks dan mengembalikannya dalam format JSON sesuai dengan model berikut:

Setiap hubungan harus memiliki field:
- subject: Entitas utama (contoh: "bank indonesia").
- subject_type: Tipe entitas, pilih salah satu dari [ORGANISASI, PEMERINTAHAN, BADAN_REGULATOR, NEGARA, KOTA, WILAYAH, ORANG, PERUSAHAAN, PRODUK, EVENT, SEKTOR, INDIKATOR_EKONOMI, INSTRUMEN_FINANSIAL, KONSEP]. Gunakan huruf kapital semua.
- relation: Hubungan antar entitas, pilih salah satu dari [Memiliki, Mengumumkan, BeroperasiDi, Memperkenalkan, Menghasilkan, Mengendalikan, Berpartisipasi, Mempengaruhi, BerdampakPositif, BerdampakNegatif, Mengaitkan, AnggotaDari, BerinvestasiDi, Meningkatkan, Menurunkan].
- object: Entitas target.
- object_type: Tipe entitas target, dengan pilihan yang sama seperti subject_type.

Aturan:
1. Jangan menambah field atau mengganti nama field.
2. Hanya keluarkan data yang ada dalam teks tanpa menciptakan informasi baru.
3. Jika tidak ada hubungan yang terdeteksi, keluarkan "[]" (sebuah array kosong).
4. Pastikan semua nilai sesuai dengan daftar yang telah disediakan dan menggunakan huruf kapital.

Contoh output:
[{{ 
  "subject": "bank indonesia", 
  "subject_type": "BADAN_REGULATOR",
  "relation": "Mengendalikan",
  "object": "suku bunga",
  "object_type": "INSTRUMEN_FINANSIAL"
}}]

Pastikan output yang Anda kembalikan adalah JSON yang valid.
""".strip()


prompt = ChatPromptTemplate.from_messages([
    ("system", system_prompt),
    ("user", "{input}")
])
parser = JsonOutputKeyToolsParser(key_name="InvestmentNewsEntities", first_tool_only=True)
chain = prompt | model | parser

BATCH_SIZE = 20
MAX_ARTICLE_RETRIES = 5     # Maximum retries per article
RETRY_DELAY = 10            # Seconds between retries
TIMEOUT_PER_ARTICLE = 300   # 5 minutes timeout per article

# Helper function to process a single article with a timeout and retries.
async def process_single_article(article_text, pub_date):
    attempts = 0
    while attempts < MAX_ARTICLE_RETRIES:
        try:
            result = await asyncio.wait_for(chain.abatch([{"input": article_text}]), timeout=TIMEOUT_PER_ARTICLE)
            result = result[0]
            if isinstance(result, dict):
                parsed = result
            else:
                try:
                    parsed = json.loads(result) if result else {}
                except json.JSONDecodeError:
                    parsed = {}
            return parsed, pub_date
        except asyncio.TimeoutError:
            attempts += 1
            print(f"Timeout: Article processing exceeded 5 minutes, attempt {attempts}/{MAX_ARTICLE_RETRIES} for article with pub_date {pub_date}")
            await asyncio.sleep(RETRY_DELAY)
        except Exception as e:
            attempts += 1
            print(f"Error: {e}, attempt {attempts}/{MAX_ARTICLE_RETRIES} for article with pub_date {pub_date}")
            await asyncio.sleep(RETRY_DELAY)
    return {"error": f"Failed after {MAX_ARTICLE_RETRIES} attempts", "content": article_text}, pub_date

# Function to update the checkpoint file.
def update_checkpoint(last_index):
    checkpoint_data = {"last_processed_index": last_index}
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(checkpoint_data, f, ensure_ascii=False, indent=4)

# Open the output file in append mode so that existing content is not deleted.
with open(JSON_OUTPUT_PATH, "a", encoding="utf-8") as f_out:
    async def process_all_data(batch_size=BATCH_SIZE):
        num_articles = len(data_to_process)
        batches = [data_to_process.iloc[i : i + batch_size] for i in range(0, num_articles, batch_size)]
        global_index = last_processed_index  # This represents the row number in the complete dataset.
        
        for batch in tqdm(batches, desc="Processing articles", unit="batch"):
            batch_inputs = [{"input": row["isi"]} for _, row in batch.iterrows()]
            batch_dates = [row["tanggal"] for _, row in batch.iterrows()]
            
            try:
                batch_results = await chain.abatch(batch_inputs)
            except Exception as e:
                print("Batch-level error, falling back to individual processing:", e)
                batch_results = [None] * len(batch_inputs)
            
            for idx, (article_input, pub_date) in enumerate(zip(batch_inputs, batch_dates)):
                global_index += 1  # Increment the overall row index.
                result = batch_results[idx]
                if result is None or not (isinstance(result, dict) and ("entities" in result or isinstance(result, list))):
                    result, pub_date = await process_single_article(article_input["input"], pub_date)
                if isinstance(result, dict) and "entities" in result:
                    entities = result["entities"]
                elif isinstance(result, list):
                    entities = result
                else:
                    entities = []
                if entities:
                    for entity in entities:
                        entity["date"] = pub_date
                        f_out.write(json.dumps(entity, ensure_ascii=False) + "\n")
                        f_out.flush()
                else:
                    error_obj = {"error": "parsing error", "date": pub_date, "content": result}
                    f_out.write(json.dumps(error_obj, ensure_ascii=False) + "\n")
                    f_out.flush()
                update_checkpoint(global_index)
            await asyncio.sleep(random.uniform(0.3, 0.7))
        
        print("Process completed. Data saved to:", JSON_OUTPUT_PATH)
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
            print("Checkpoint file removed.")
    
    asyncio.run(process_all_data(batch_size=BATCH_SIZE))
