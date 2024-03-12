from prefect import flow, task
import requests
from bs4 import BeautifulSoup

@task
def get_wikipedia_page(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    return soup

@task
def extract_stadium_data(soup):
    table = soup.find("table", {"class": "wikitable"})
    rows = table.find_all("tr")
    headers = [header.text.strip() for header in rows[1].find_all("th")]
    data = []
    for row in rows[2:]:
        cols = row.find_all("td")
        cols = [col.text.strip() for col in cols]
        if cols:
            stadium_data = dict(zip(headers, cols))
            data.append(stadium_data)
    return data

@flow
def wikipedia_flow(url: str = "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"):
    soup = get_wikipedia_page(url)
    stadium_data = extract_stadium_data(soup)
    print(stadium_data)

if __name__ == "__main__":
    wikipedia_flow()