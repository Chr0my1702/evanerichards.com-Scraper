from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import pandas as pd
import requests
from bs4 import BeautifulSoup


def get_film_urls(base_url, final):
    urls = [f"{base_url}{i}" for i in range(1, final)]
    href_list = []

    for url in urls:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        elements = soup.find_all("a", class_="elementor-post__thumbnail__link")

        for element in elements:
            href_list.append(element["href"])
    return href_list


four_k_films = get_film_urls(
    "https://www.evanerichards.com/4k-movie-stills", 1)
hd_films = get_film_urls("https://www.evanerichards.com/hd-movie-stills/", 17)
sd_films = get_film_urls("https://www.evanerichards.com/sd-movie-stills/", 1)

films = four_k_films + hd_films + sd_films


def get_image_attributes(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    elements = soup.find_all(class_="lazyload")
    image_data = []

    for element in elements:

        title = element.get("title", "")
        if title != "":
            alt = element.get("alt", "")
            width = element.get("width", "")
            height = element.get("height", "")
            style = element.get("style", "")
            data_src = element.get("data-src", "")

            image_data.append({
                "title": title,
                "alt": alt,
                "width": width,
                "height": height,
                "style": style,
                "data-src": data_src
            })
    return image_data


def get_all_image_data(films):
    all_image_data = []

    with ThreadPoolExecutor() as executor, tqdm(total=len(films)) as progress_bar:
        futures = [executor.submit(get_image_attributes, film_url)
                   for film_url in films]
        for future in futures:
            all_image_data += future.result()
            progress_bar.update(1)

    return pd.DataFrame(all_image_data)


df = get_all_image_data(films)


df.to_csv("film_data.csv")
df.to_parquet("film_data.parquet", engine="pyarrow")
