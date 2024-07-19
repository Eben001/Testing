import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import re
import urllib3
import asyncio
import aiohttp
from aiohttp import ClientSession
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import os

# if asyncio.get_event_loop().is_running():
#     import nest_asyncio
#     nest_asyncio.apply()

USERNAME, PASSWORD = os.environ['username'], os.environ['password']


proxies = f'http://{USERNAME}:{PASSWORD}@unblock.oxylabs.io:60000'



profile_list = []
async def get_profile_details_with_retry(session, url):
  retry_attempts = 20
  for attempt in range(retry_attempts):
    try:
      response = await session.get(url, proxy=proxies, ssl=False)
      if response.status == 200:
        # print('Got it: ', response.status)
        return response
      elif str(response.status).startswith('4') and response.status != 404:
        print(f"4xx error encountered: {response.status}. Retrying...")
      elif str(response.status).startswith('5'):
        print(f"5xx error encountered: {response.status}. Retrying... Attempt {attempt+1}/{retry_attempts}")
        sleep_time = attempt * 2
        print(f"Retrying in {sleep_time} seconds...")
        await asyncio.sleep(sleep_time)
      else:
        print(f"Unexpected error encountered: {response.status}. Retrying...")
    except:
      print("Error, retrying....")
      pass


async def get_profile_details(session, booking_url):
    response = await get_profile_details_with_retry(session, booking_url)


    soup = BeautifulSoup(await response.text(), 'lxml')
    data = None
    name = None
    age = None
    height = None
    weight = None
    race = None
    sex = None
    arrested_by = None
    booked_date = None
    county_url = None
    state = ''  # Initialize state here
    county = ''  # Initialize county here
    charges = None
    image_url = None
    image_name = None
    length = None

    try:
        data = soup.find('h1', class_='post-title item fn').text.strip()
    except:
        pass

    try:
        name = soup.find('td', itemprop='name').get_text(strip=True)
    except:
        pass

    try:
        age = soup.find('th', string='age').find_next_sibling('td').get_text(strip=True)
    except:
        pass

    try:
        height = soup.find('th', string='height').find_next_sibling('td').get_text(strip=True)
    except:
        pass

    try:
        weight = soup.find('th', string='weight').find_next_sibling('td').get_text(strip=True)
    except:
        pass

    try:
        race = soup.find('th', string='race').find_next_sibling('td').get_text(strip=True)
    except:
        pass

    try:
        sex = soup.find('td', itemprop='gender').get_text(strip=True)
    except:
        pass

    try:
        arrested_by = soup.find('th', string='arrested by').find_next_sibling('td').get_text(strip=True)
    except:
        pass

    try:
        booked_date = soup.find('th', string='booked').find_next_sibling('td').get_text(strip=True)
    except:
        pass

    try:
        county_url = soup.find('span', class_='cats').find('a')['href']
    except:
        pass

    try:
        script_tag = soup.find('script', string=re.compile(r'function\s+__GA4'))
        if script_tag:
            script_content = script_tag.string
            match = re.search(r"'content_group':\s*'([^']+)'", script_content)
            if match:
                state_county = match.group(1).split('/')
                if len(state_county) == 2:
                    state = state_county[0].strip()
                    county = state_county[1].strip()
    except:
        pass

    try:
        charges_list = []
        charges_tables = soup.find_all('table', {'border': '1'})
        for table in charges_tables:
            charge_desc = table.find('th', string='charge description').find_next_sibling('td').get_text(strip=True)
            charges_list.append(charge_desc)
        charges = ":::\n".join(charges_list)
    except:
        pass

    try:
        image_url = soup.find('meta', {"property": "og:image"})['content']
        image_name = image_url.split('/')[-1].split('.')[0]
    except:
        pass

    try:
        #count the characters of the charges text
        length = len(charges)
    except:
        pass


    profile = {
        'Name': name,
        'Age': age,
        'Race': race,
        'Sex': sex,
        'Arrested By': arrested_by,
        'Booked Date': booked_date,
        'Height': height,
        'Weight': weight,
        'Image URL': image_url,
        'Image Name': image_name,
        'State': state,
        'County': county,
        'Data': data,
        'County URL': county_url,
        'Booking Page URL': booking_url,
        'Charges': charges,
        'Length': length
    }
    print(name)
    profile_list.append(profile)


async def get_last_page(session, url):
  # url = 'https://bustednewspaper.com/mugshots/florida/alachua-county/'

  for _ in range(20):
    try:
        response = await session.get(url, proxy=proxies, ssl=False)
        if response.status == 200:
          # print('Got it: ', response.status)
          soup = BeautifulSoup(await response.text(), 'lxml')
          page_numbers = soup.find_all('a', class_='page-numbers')
          max_page = 0
          for page in page_numbers:
              page_num = int(re.search(r'page/(\d+)/', page['href']).group(1))
              if page_num > max_page:
                  max_page = page_num

          return max_page
        

    except:
      print("Error, retrying....")
      pass




async def get_pages_with_retry(session, url):
  retry_attempts = 20
  for attempt in range(retry_attempts):
    try:
      response = await session.get(url, proxy=proxies, ssl=False)
      if response.status == 200:
        print('Got it: ', response.status)
        return response
      elif str(response.status).startswith('4') and response.status != 404:
        print(f"4xx error encountered: {response.status}. Retrying...")
      elif str(response.status).startswith('5'):
        print(f"5xx error encountered: {response.status}. Retrying... Attempt {attempt+1}/{retry_attempts}")
        sleep_time = attempt * 2
        print(f"Retrying in {sleep_time} seconds...")
        await asyncio.sleep(sleep_time)
      else:
        print(f"Unexpected error encountered: {response.status}. Retrying...")
    except:
      print("Error, retrying....")
      pass

async def get_start_urls(session, url):
   counties_links = []
   for _ in range(20):
    try:
        response = await session.get(url, proxy=proxies, ssl=False)
        if response.status == 200:
          soup = BeautifulSoup(await response.text(), 'lxml')
          ol = soup.find('ol', class_='counties')
          all_counties = ol.find_all('h3')
          for county in all_counties[0:3]:
            county_link = county.find('a')['href']
            # print(county_link)
            counties_links.append(county_link)

          return counties_links  
          break
    except:
      print("Error, retrying....")
      pass

  

async def main():
    try:
        async with ClientSession() as session:
            state_url = 'https://bustednewspaper.com/mugshots/ohio/'
            state = state_url.split('/')[-2]
            print(state)
            counties_links = await get_start_urls(session, state_url)
            for county in counties_links:
                start_url = county
                county = start_url.split('/')[-2]
               
                print(f"Currently scraping county {county}")

                last_page = await get_last_page(session, start_url)
                print("Last Page: ", last_page)
                last_page = 1

                tasks = []
                for page_num in range(1, last_page + 1):
                    url = f'{start_url}page/{page_num}/'
                    print("Currently scraping page: ", page_num)
                    response = await get_pages_with_retry(session, url)
                    soup = BeautifulSoup(await response.text(), 'lxml')

                    listings_div = soup.find('div', class_='posts-list listing-alt')
                    articles = listings_div.find_all('article')
                    for article in articles:
                        try:
                            content = article.find('div', class_='content')
                            link = content.find('a')['href']
                            tasks.append(get_profile_details(session, link))
                        except:
                            link = None

                await asyncio.gather(*tasks)

    except KeyboardInterrupt:
        print("Received KeyboardInterrupt. Stopping gracefully.")
    except Exception as e:
        print("Error: ", e)
    finally:
        global_df = pd.DataFrame(profile_list)
        output_file = f'profiles_{state}.csv'
        global_df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")
        profile_list.clear()



if __name__ == "__main__":
    asyncio.run(main())

