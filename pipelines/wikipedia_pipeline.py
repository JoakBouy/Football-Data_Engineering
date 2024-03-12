import requests
from bs4 import BeautifulSoup
import pandas as pd
from geopy.geocoders import Nominatim
import prefect

NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'


def get_wikipedia_page(url):
    """
    Fetches the content of a Wikipedia page using requests.

    Args:
        url (str): The URL of the Wikipedia page to retrieve.

    Returns:
        str: The HTML content of the page, or None if an error occurs.
    """

    print(f"Getting wikipedia page...", url)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Check for successful request

        return response.text
    except requests.RequestException as e:
        print(f"An error occurred fetching the page: {e}")
        return None


def get_wikipedia_data(html):
    """
    Parses the HTML content and extracts table rows containing stadium data.

    Args:
        html (str): The HTML content of a Wikipedia page.

    Returns:
        list: A list of table row elements (TR), or None if no table is found.
    """

    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all("table", {"class": ["wikitable", "sortable", "jquery-tablesorter"]})  # Update class names

    if tables:
        return tables[0].find_all('tr')
    else:
        print("No table found")
        return None



def clean_text(text):
    """
    Cleans text by removing unwanted characters and formatting.

    Args:
        text (str): The text to be cleaned.

    Returns:
        str: The cleaned text.
    """

    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]

    return text.replace('\n', '')


def extract_wikipedia_data(html):
    """
    Parses the HTML content and extracts table rows containing stadium data.

    Args:
        html (str): The HTML content of a Wikipedia page.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted stadium data, 
                      or None if no table is found.
    """

    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all("table", {"class": ["wikitable", "sortable", "jquery-tablesorter"]})  # Updated class names

    if tables:
        rows = tables[0].find_all('tr')
    else:
        print("No table found")
        return None

    data = []
    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        values = {}

        # Check length of tds before accessing elements
        if len(tds) >= 3:
            values['rank'] = i
            values['stadium'] = clean_text(tds[0].text)
            values['capacity'] = clean_text(tds[1].text).replace(',', '').replace('.', '')
            values['region'] = clean_text(tds[2].text) if len(tds) > 2 else ''  # Handle missing region
            values['country'] = clean_text(tds[3].text) if len(tds) > 3 else ''  # Optional: Handle missing country
            values['city'] = clean_text(tds[4].text) if len(tds) > 4 else ''  # Optional: Handle missing city
            values['images'] = 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else NO_IMAGE
            values['home_team'] = clean_text(tds[6].text) if len(tds) > 6 else ''  # Optional: Handle missing home team
        else:
            print(f"Row {i} has less than 3 data cells, skipping")

        data.append(values)

    return pd.DataFrame(data)



def get_lat_long(country, city):
    """
    Geocodes a location using Nominatim.

    Args:
        country (str): The country of the location.
        city (str): The city of the location.

    Returns:
        tuple: A tuple containing latitude and longitude (or None if geocoding fails).
    """

    geolocator = Nominatim(user_agent='geoapiExercises')
    location = geolocator.geocode(f'{city}, {country}')

    if location:
        return location.latitude, location.longitude

    return None


def transform_wikipedia_data(data):
    """
    Transforms and cleans the extracted Wikipedia data.

    Args:
        data (pd.DataFrame): The DataFrame containing stadium data.

    Returns:
        pd.DataFrame: The transformed DataFrame with additional columns.
    """

    stadiums_df = data.copy()
    stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE)
    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    # Handle duplicates (assuming location is the unique identifier)
    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    stadiums_df.update(duplicates)

    return stadiums_df


@prefect.flow
def wikipedia_data_pipeline(url: str, output_file: str):
    """
    Prefect flow to extract, transform, and write Wikipedia stadium data to a CSV file.

    Args:
        url (str): The URL of the Wikipedia page containing stadium data.
        output_file (str): The path to the output CSV file.
    """

    data = extract_wikipedia_data(url)

    if data is not None:
        transformed_data = transform_wikipedia_data(data)
        transformed_data.to_csv(output_file, index=False)
        print(f"Data written to CSV file: {output_file}")


if __name__ == '__main__':
    wikipedia_data_pipeline(
        url="https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity",
        output_file="stadiums.csv"
    )
