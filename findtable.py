import requests
from bs4 import BeautifulSoup
import pandas as pd

def clean_text(text):
  """
  This function removes extra characters and whitespaces from text.
  """
  return text.strip().replace('\n', '')

def get_wikipedia_data(url):
  """
  Fetches the HTML content of a Wikipedia page and extracts table rows 
  containing stadium data, handling potential variations in table structure.

  Args:
      url (str): The URL of the Wikipedia page.

  Returns:
      pd.DataFrame: A DataFrame containing the extracted stadium data, 
                     or None if no table is found.
  """

  response = requests.get(url)
  html = response.content
  soup = BeautifulSoup(html, 'html.parser')

  
  tables = soup.find_all("table", {"class": ["wikitable", "sortable", "jquery-tablesorter"]})  # Original approach
  if not tables:
      tables = soup.find_all("table", {"class": ["wikitable", "standard"]})  # Alternative class names 
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
        
      else:
          print(f"Row {i} has less than 3 data cells, skipping")

      data.append(values)

  return pd.DataFrame(data)


url = "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"

data = get_wikipedia_data(url)

if data is not None:
  # Print the first few rows of the DataFrame (optional)
  print(data.head()) 
else:
  print("No data found")
