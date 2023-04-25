import os
import pandas as pd

csv_file_path = 'film_data.csv'
df = pd.read_csv(csv_file_path)

base_folder = 'all'

missing_files = []

min_file_size = 1024  # 1 KB

for index, row in df.iterrows():
    title = row['title']

    if '_' in title:
        cleaned_title = title.rsplit('_', 1)[0]
    else:
        cleaned_title = ''.join(c for c in title if not c.isdigit())

    folder_path = os.path.join(base_folder, cleaned_title)
    filename = f"{title}.jpg"
    stripped_folder_name = cleaned_title.replace(' ', '')
    image_file_path = "all.".replace('.', '\\') + os.path.join(
        stripped_folder_name, filename.replace(' ', ''))

    if not os.path.exists(image_file_path) or os.path.getsize(image_file_path) < min_file_size:
        missing_files.append((index, title, image_file_path))
        if os.path.exists(image_file_path) and os.path.getsize(image_file_path) < min_file_size:
            os.remove(image_file_path)

with open('missing_files.txt', 'w') as f:
    for index, title, image_file_path in missing_files:
        f.write(
            f"Row: {index}, Title: {title}, Image File Path: {image_file_path}\n")