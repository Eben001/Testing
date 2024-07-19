import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import re
import urllib3
import asyncio
import aiohttp
from aiohttp import ClientSession
import os
from fake_useragent import UserAgent
from telegram import Bot

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# if asyncio.get_event_loop().is_running():
#     import nest_asyncio
#     nest_asyncio.apply()
  
USERNAME, PASSWORD = os.environ['username'], os.environ['password']


proxies = f'http://{USERNAME}:{PASSWORD}@rp.proxyscrape.com:6060'
telegram_token = os.environ['telegram_token']
telegram_chat_id = os.environ['telegram_chat_id']
bot = Bot(token=telegram_token)


async def send_file_to_telegram(filename):
    with open(filename, 'rb') as file:
        await bot.send_document(chat_id=telegram_chat_id, document=file)

async def send_telegram_message(text):
    await bot.send_message(chat_id=telegram_chat_id, text=text)



profile_list = []
async def get_profile_details_with_retry(session, url):
  #proxies = 'http://vk4ybm4f7ps5a54-odds-5+100:teys8oi11uhte8f@rp.proxyscrape.com:6060'

  retry_attempts = 20
  for attempt in range(retry_attempts):
      headers = {
    'User-Agent': ua.random,
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Sec-Purpose': 'prefetch',
    'Connection': 'keep-alive',
    'Referer': 'https://bustednewspaper.com/mugshots/ohio/adams-county/',
    'Cookie': 'usprivacy=1N--; _ga_PHJMBM9BQV=GS1.1.1721344369.6.1.1721344749.42.0.0; _ga=GA1.1.97208457.1721307241; _fbp=fb.1.1721307248354.736019126636307686',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'no-cors',
    'Sec-Fetch-Site': 'same-origin',
    'If-None-Match': '"4f672d81cfc0faf51038eb00308474bc"'}


      response = await session.get(url, proxy=proxies,headers=headers)
      if response.status == 200:
        # print('Got it: ', response.status)
        return response
      elif str(response.status).startswith('4') and response.status != 404:
        print(f"4xx error encountered: {response.status}. Retrying...")
        async with aiohttp.ClientSession() as new_session:
          session = new_session  # Replace the session with a new on
          sleep_time = attempt * 2
          print(f"Retrying in {sleep_time} seconds...")
          await asyncio.sleep(sleep_time)

      elif str(response.status).startswith('5'):
        async with aiohttp.ClientSession() as new_session:
          session = new_session  # Replace the session with a new on
          sleep_time = attempt * 2
          print(f"Retrying in {sleep_time} seconds...")
          await asyncio.sleep(sleep_time)

      else:
        print(f"Unexpected error encountered: {response.status}. Retrying...")



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
  #proxies = 'http://vk4ybm4f7ps5a54-odds-5+100:teys8oi11uhte8f@rp.proxyscrape.com:6060'
 
  retry_attempts= 20
  for attempt in range(retry_attempts):
    headers = {
    'User-Agent': ua.random,
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Sec-Purpose': 'prefetch',
    'Connection': 'keep-alive',
    'Referer': 'https://bustednewspaper.com/mugshots/ohio/adams-county/',
    'Cookie': 'usprivacy=1N--; _ga_PHJMBM9BQV=GS1.1.1721344369.6.1.1721344749.42.0.0; _ga=GA1.1.97208457.1721307241; _fbp=fb.1.1721307248354.736019126636307686',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'no-cors',
    'Sec-Fetch-Site': 'same-origin',
    'If-None-Match': '"4f672d81cfc0faf51038eb00308474bc"',}


    try:
      response = await session.get(url, proxy=proxies,headers=headers)
      if response.status == 200:
        # print('Got it: ', response.status)
        soup = BeautifulSoup(await response.text(), 'lxml')
        try:
          page_numbers = soup.find_all('a', class_='page-numbers')
          max_page = 0
          for page in page_numbers:
              page_num = int(re.search(r'page/(\d+)/', page['href']).group(1))
              if page_num > max_page:
                  max_page = page_num

          return max_page
        except:
          pass
      elif str(response.status).startswith('4') and response.status != 404:
        print(f"4xx error encountered: {response.status}. Retrying...")
        #async with aiohttp.ClientSession() as new_session:
          #session = new_session  # Replace the session with a new on
        sleep_time = attempt * 2
        print(f"Retrying in {sleep_time} seconds...")
        await asyncio.sleep(sleep_time)
          #continue
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




async def get_pages_with_retry(session, url):
  retry_attempts = 20
  for attempt in range(retry_attempts):
    proxies = 'http://vk4ybm4f7ps5a54-odds-5+100:teys8oi11uhte8f@rp.proxyscrape.com:6060'
 
    headers = {
    'User-Agent': ua.random,
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Sec-Purpose': 'prefetch',
    'Connection': 'keep-alive',
    'Referer': 'https://bustednewspaper.com/mugshots/ohio/adams-county/',
    'Cookie': 'usprivacy=1N--; _ga_PHJMBM9BQV=GS1.1.1721344369.6.1.1721344749.42.0.0; _ga=GA1.1.97208457.1721307241; _fbp=fb.1.1721307248354.736019126636307686',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'no-cors',
    'Sec-Fetch-Site': 'same-origin',
    'If-None-Match': '"4f672d81cfc0faf51038eb00308474bc"',

}
    try:
      response = await session.get(url, proxy=proxies,headers=headers)
      if response.status == 200:
        print('Got it: ', response.status)
        return response
      elif str(response.status).startswith('4') and response.status != 404:
        print(f"4xx error encountered: {response.status}. Retrying...")
        #async with aiohttp.ClientSession() as new_session:
         # session = new_session  # Replace the session with a new on
        sleep_time = attempt * 2
        print(f"Retrying in {sleep_time} seconds...")
        await asyncio.sleep(sleep_time)
          #continue
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
  retry_attempts = 20
  for attempt in range(retry_attempts):

    proxies = 'http://vk4ybm4f7ps5a54-odds-5+100:teys8oi11uhte8f@rp.proxyscrape.com:6060'
    cookies = {
    'usprivacy': '1N--',
    '_ga_PHJMBM9BQV': 'GS1.1.1721344369.6.1.1721344749.42.0.0',
    '_ga': 'GA1.1.97208457.1721307241',
    '_fbp': 'fb.1.1721307248354.736019126636307686',
}

    headers = {
    'User-Agent': ua.random,
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Sec-Purpose': 'prefetch',
    'Connection': 'keep-alive',
    'Referer': 'https://bustednewspaper.com/mugshots/ohio/adams-county/',
    'Cookie': 'usprivacy=1N--; _ga_PHJMBM9BQV=GS1.1.1721344369.6.1.1721344749.42.0.0; _ga=GA1.1.97208457.1721307241; _fbp=fb.1.1721307248354.736019126636307686',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'no-cors',
    'Sec-Fetch-Site': 'same-origin',
    'If-None-Match': '"4f672d81cfc0faf51038eb00308474bc"',

}
    try:
      response = await session.get(url, proxy=proxies,headers=headers)
      if response.status == 200:
        soup = BeautifulSoup(await response.text(), 'lxml')
        try:
          ol = soup.find('ol', class_='counties')
          all_counties = ol.find_all('h3')
          for county in all_counties[0:1]: #0:26
            county_link = county.find('a')['href']
            # print(county_link)
            counties_links.append(county_link)

          return counties_links
        except:
          pass
      elif str(response.status).startswith('4') and response.status != 404:
        print(f"4xx error encountered: {response.status}. Retrying...")
        #async with aiohttp.ClientSession() as new_session:
          #session = new_session  # Replace the session with a new on
        sleep_time = attempt * 2
        print(f"Retrying in {sleep_time} seconds...")
        await asyncio.sleep(sleep_time)
          #continue
      elif str(response.status).startswith('5'):
        print(f"5xx error encountered: {response.status}. Retrying... Attempt {attempt+1}/{retry_attempts}")
        sleep_time = attempt * 2
        print(f"Retrying in {sleep_time} seconds...")
        await asyncio.sleep(sleep_time)
      else:
        print(f"Unexpected error encountered: {response.status}. Retrying...")
    except:
      print("Error, retrying....")



async def main():
    BATCH_SIZE = 10  # Define your batch size

    try:
        async with ClientSession() as session:
            state_url = 'https://bustednewspaper.com/mugshots/ohio/'
            state = state_url.split('/')[-2]
            await send_telegram_message(f'Started {state}')

            print(state)
            counties_links = await get_start_urls(session, state_url)
            for county in counties_links:
                start_url = county
                county = start_url.split('/')[-2]

                print(f"Currently scraping county: {county}")

                last_page = await get_last_page(session, start_url)
                # print("Last Page: ", last_page)
                last_page = 2

                for page_num in range(1, last_page + 1, BATCH_SIZE):
                    batch_tasks = []
                    for batch_page_num in range(page_num, min(page_num + BATCH_SIZE, last_page + 1)):
                        url = f'{start_url}page/{batch_page_num}/'
                        print(f"Currently scraping page: {batch_page_num}")
                        try:
                          response = await get_pages_with_retry(session, url)
                          soup = BeautifulSoup(await response.text(), 'lxml')

                          listings_div = soup.find('div', class_='posts-list listing-alt')
                          articles = listings_div.find_all('article')
                          for article in articles:
                              try:
                                  content = article.find('div', class_='content')
                                  link = content.find('a')['href']
                                  batch_tasks.append(get_profile_details(session, link))
                              except:
                                  link = None
                        except:
                          pass
                    await asyncio.gather(*batch_tasks)

    except KeyboardInterrupt:
        print("Received KeyboardInterrupt. Stopping gracefully.")
        await send_telegram_message('Received KeyboardInterrupt. Stopping gracefully')

    except Exception as e:
        print("Error: ", e)
        await send_telegram_message(f'Exception occurred from main(): {e}')

    finally:
        global_df = pd.DataFrame(profile_list)
        output_file = f'profiles_{state}.csv'
        global_df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")
        await send_file_to_telegram(output_file)
        await send_telegram_message('Saved file')
        profile_list.clear()

if __name__ == "__main__":
    asyncio.run(main())
