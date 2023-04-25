import threading
from tqdm import tqdm
import os
import shutil
import requests
import pandas as pd
import time


def download_image(url, directory, filename):
    response = requests.get(url, stream=True)
    response.raise_for_status()

    os.makedirs(directory, exist_ok=True)
    file_path = os.path.join(directory, filename)

    with open(file_path, 'wb') as f:
        response.raw.decode_content = True
        shutil.copyfileobj(response.raw, f)


def make_all_files():
    titles = df['title'].unique()

    cleaned_titles = []
    for title in titles:
        if '_' in title:
            cleaned_title = title.rsplit('_', 1)[0]
        else:
            cleaned_title = ''.join(c for c in title if not c.isdigit())
        cleaned_titles.append(cleaned_title)
    for cleaned_title in cleaned_titles:
        folder_path = os.path.join(base_folder, cleaned_title)
        os.makedirs(folder_path, exist_ok=True)


csv_file = 'film_data.csv'
df = pd.read_csv(csv_file)
base_folder = 'all'


def download_image_with_progress(image_url, folder_path, filename, pbar):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    image_file_path = os.path.join(
        folder_path.replace(' ', ''), filename.replace(' ', ''))

    if not os.path.exists(image_file_path):
        try:
            response = requests.get(image_url)
            response.raise_for_status()
            print(image_file_path)
            with open(image_file_path, 'wb') as file:
                file.write(response.content)
        except Exception as e:
            print(f"Error downloading {image_url}: {e}")
    pbar.update(1)


def process_batch(df, start_index, end_index, base_folder):
    threads = []

    with tqdm(total=(end_index - start_index)) as pbar:
        for index, row in df.iloc[start_index:end_index].iterrows():
            title = row['title']

            if '_' in title:
                cleaned_title = title.rsplit('_', 1)[0]
            else:
                cleaned_title = ''.join(c for c in title if not c.isdigit())

            folder_path = os.path.join(base_folder, cleaned_title)
            image_url = row['data-src']
            filename = f"{title}.jpg"
            image_file_path = os.path.join(
                folder_path, filename.replace(' ', ''))

            if not os.path.exists(image_file_path):
                time.sleep(0.03)

                t = threading.Thread(target=download_image_with_progress, args=(
                    image_url, folder_path, filename, pbar))
                threads.append(t)
                t.start()

        for thread in threads:
            thread.join()

        print(f"\n{cleaned_title} folder done.")


batch_size = 50
num_batches = (len(df) + batch_size - 1) // batch_size

for i in range(num_batches):
    start_index = i * batch_size
    end_index = min((i + 1) * batch_size, len(df))
    process_batch(df, start_index, end_index, base_folder)

print("DONE")