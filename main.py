import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import re
import urllib3
import asyncio
import aiohttp
from aiohttp import ClientSession
from telegram import Bot
import requests
from fake_useragent import UserAgent
import random
import os
import base64

ua = UserAgent()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

telegram_token = os.environ['telegram_token']
telegram_chat_id = os.environ['telegram_chat_id']
bot = Bot(token=telegram_token)

async def send_file_to_telegram(filename):
    with open(filename, 'rb') as file:
        await bot.send_document(chat_id=telegram_chat_id, document=file)

async def send_telegram_message(text):
    await bot.send_message(chat_id=telegram_chat_id, text=text)




profile_list = []
async def fetch_with_retry(session, url, semaphore, max_retries=20):
    api_key = os.environ['api_key']
    data = {
        "url": url,
        "httpResponseBody": True
    }

    headers = {
        'Content-Type': 'application/json',
        'X-Api-Key': api_key
    }


    retry_count = 0
    backoff_factor = 2
    while retry_count < max_retries:
        try:
            async with semaphore:
                # response = await session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=60),proxy=proxies)
                response = await session.post('https://api.proxyscrape.com/v3/accounts/freebies/scraperapi/request', headers=headers, json=data)
            
            
            if response.status == 200:
                json_response = await response.json()
                if 'httpResponseBody' in json_response['data']:
                    # Decode the base64-encoded HTML
                    decoded_html = base64.b64decode(json_response['data']['httpResponseBody']).decode()
                    return decoded_html
            elif response.status >= 400 and response.status < 500:
                print(f"4xx error encountered: {response.status}. Retrying...")
            elif response.status >= 500:
                print(f"5xx error encountered: {response.status}. Retrying...")
        except (aiohttp.ClientOSError, aiohttp.ServerDisconnectedError) as e:
            print(f"Network error encountered: {e}. Retrying...")
        except Exception as e:
            print(f"Unexpected error encountered: {e}. Retrying...")

        retry_count += 1
        sleep_time = backoff_factor ** retry_count + random.uniform(0, 1)
        print(f"Retrying in {sleep_time} seconds...")
        await asyncio.sleep(sleep_time)
    return None


async def get_profile_details(session, booking_url, semaphore):
    # headers = {
    #     'User-Agent': ua.random,
    #     'Accept': '*/*',
    #     'Accept-Language': 'en-US,en;q=0.5',
    #     'Accept-Encoding': 'gzip, deflate, br',
    #     'Sec-Purpose': 'prefetch',
    #     'Connection': 'keep-alive',
    #     'Referer': 'https://bustednewspaper.com/mugshots/ohio/adams-county/',
    #     'Cookie': 'usprivacy=1N--; _ga_PHJMBM9BQV=GS1.1.1721344369.6.1.1721344749.42.0.0; _ga=GA1.1.97208457.1721307241; _fbp=fb.1.1721307248354.736019126636307686',
    #     'Sec-Fetch-Dest': 'empty',
    #     'Sec-Fetch-Mode': 'no-cors',
    #     'Sec-Fetch-Site': 'same-origin',
    #     'If-None-Match': '"4f672d81cfc0faf51038eb00308474bc"'
    # }
    response = await fetch_with_retry(session, booking_url, semaphore)
    if response is None:
        return

    soup = BeautifulSoup(response, 'lxml')
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
        try:
          name = soup.find('h1', class_='post-title item fn').text.strip()
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
        try:
          sex = soup.find('th', string='sex').find_next_sibling('td').get_text(strip=True)
        except:
          pass

    try:
        arrested_by = soup.find('th', string='arrested by').find_next_sibling('td').get_text(strip=True)
    except:
        try:
          arrested_by = soup.find('th', string='arrested').find_next_sibling('td').get_text(strip=True)
        except:
          pass

    try:
        booked_date = soup.find('th', string='booked').find_next_sibling('td').get_text(strip=True)
    except:
        try:
          booked_date = soup.find('time', class_='value-title').text
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
        try:
          charges_table = soup.find('h2', id='booking-charges-header').find_next('table')

          charge_descriptions = [row.find('td').text for row in charges_table.find_all('tr')[1:]]

          charges = ':::\n'.join(charge_descriptions)
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


async def get_last_page(session, url, semaphore):
    # headers = {
    #     'User-Agent': ua.random,
    #     'Accept': '*/*',
    #     'Accept-Language': 'en-US,en;q=0.5',
    #     'Accept-Encoding': 'gzip, deflate, br',
    #     'Sec-Purpose': 'prefetch',
    #     'Connection': 'keep-alive',
    #     'Referer': 'https://bustednewspaper.com/mugshots/ohio/adams-county/',
    #     'Cookie': 'usprivacy=1N--; _ga_PHJMBM9BQV=GS1.1.1721344369.6.1.1721344749.42.0.0; _ga=GA1.1.97208457.1721307241; _fbp=fb.1.1721307248354.736019126636307686',
    #     'Sec-Fetch-Dest': 'empty',
    #     'Sec-Fetch-Mode': 'no-cors',
    #     'Sec-Fetch-Site': 'same-origin',
    #     'If-None-Match': '"4f672d81cfc0faf51038eb00308474bc"'
    # }
    response = await fetch_with_retry(session, url, semaphore)
    if not response:
        return

    soup = BeautifulSoup(response, 'lxml')
    max_page = 0
    for page in soup.find_all('a', class_='page-numbers'):
        page_num = int(re.search(r'page/(\d+)/', page['href']).group(1))
        if page_num > max_page:
            max_page = page_num
    return max_page


async def get_start_urls(session, url, semaphore):
    # headers = {
    #     'User-Agent': ua.random,
    #     'Accept': '*/*',
    #     'Accept-Language': 'en-US,en;q=0.5',
    #     'Accept-Encoding': 'gzip, deflate, br',
    #     'Sec-Purpose': 'prefetch',
    #     'Connection': 'keep-alive',
    #     'Referer': 'https://bustednewspaper.com/mugshots/ohio/adams-county/',
    #     'Cookie': 'usprivacy=1N--; _ga_PHJMBM9BQV=GS1.1.1721344369.6.1.1721344749.42.0.0; _ga=GA1.1.97208457.1721307241; _fbp=fb.1.1721307248354.736019126636307686',
    #     'Sec-Fetch-Dest': 'empty',
    #     'Sec-Fetch-Mode': 'no-cors',
    #     'Sec-Fetch-Site': 'same-origin',
    #     'If-None-Match': '"4f672d81cfc0faf51038eb00308474bc"'
    # }
    response = await fetch_with_retry(session, url, semaphore)
    if not response:
        return []

    soup = BeautifulSoup(response, 'lxml')
    counties_links = [county.find('a')['href'] for county in soup.find('ol', class_='counties').find_all('h3')]
    return counties_links

async def main():
    BATCH_SIZE = 5
    try:
        # connector = aiohttp.TCPConnector(force_close=True)
        async with ClientSession() as session:
            semaphore = asyncio.Semaphore(10)
            state_url = 'https://bustednewspaper.com/mugshots/ohio/'
            state = state_url.split('/')[-2]
            # await send_telegram_message(f'Started {state}')

            print(state)
            counties_links = await get_start_urls(session, state_url, semaphore)
            # print(counties_links[1:6])

            # for county in counties_links[3:4]:

            start_url = counties_links[30]
            county = start_url.split('/')[-2]

            print(f"Currently scraping county: {county}")

            last_page = await get_last_page(session, start_url, semaphore)
            print("Last Page: ", last_page)
            last_page = 2

            for page_num in range(1, last_page + 1, BATCH_SIZE):
                batch_tasks = []
                for batch_page_num in range(page_num, min(page_num + BATCH_SIZE, last_page + 1)):
                    url = f'{start_url}page/{batch_page_num}/'
                    print(f"Currently scraping page: {batch_page_num} of {last_page} - {county}")
                    try:
                      response = await fetch_with_retry(
                          session, url, 
                          #  {
                          #           'User-Agent': ua.random,
                          #           'Accept': '*/*',
                          #           'Accept-Language': 'en-US,en;q=0.5',
                          #           'Accept-Encoding': 'gzip, deflate, br',
                          #           'Sec-Purpose': 'prefetch',
                          #           'Connection': 'keep-alive',
                          #           'Referer': 'https://bustednewspaper.com/mugshots/ohio/adams-county/',
                          #           'Cookie': 'usprivacy=1N--; _ga_PHJMBM9BQV=GS1.1.1721344369.6.1.1721344749.42.0.0; _ga=GA1.1.97208457.1721307241; _fbp=fb.1.1721307248354.736019126636307686',
                          #           'Sec-Fetch-Dest': 'empty',
                          #           'Sec-Fetch-Mode': 'no-cors',
                          #           'Sec-Fetch-Site': 'same-origin',
                          #           'If-None-Match': '"4f672d81cfc0faf51038eb00308474bc"'
                          #       }, 
                          semaphore)
                      soup = BeautifulSoup(response, 'lxml')

                      listings_div = soup.find('div', class_='posts-list listing-alt')
                      articles = listings_div.find_all('article')
                      for article in articles:
                          try:
                              content = article.find('div', class_='content')
                              link = content.find('a')['href']
                              batch_tasks.append(get_profile_details(session, link, semaphore))
                          except:
                              link = None
                    except:
                      pass
                await asyncio.gather(*batch_tasks)

    except KeyboardInterrupt:
        print("Received KeyboardInterrupt. Stopping gracefully.")
        await send_telegram_message('Received KeyboardInterrupt. Stopping gracefully')

    except (
      aiohttp.ClientOSError,
      aiohttp.ServerDisconnectedError) as e:
      print("Error from main(), ClientOSError Or ServerDisconnectedError, retrying....", e)
      await asyncio.sleep(3 + random.randint(0, 9))

    except Exception as e:
        print("Exception from main(): ", e)
        await send_telegram_message(f'Exception occurred from main(): {e}')
    finally:
        global_df = pd.DataFrame(profile_list)
        output_file = f'profiles_{state}_{county}.csv'
        global_df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")
        await send_file_to_telegram(output_file)
        await send_telegram_message(f'Done for county: {county}')
        profile_list.clear()


if __name__ == "__main__":
    asyncio.run(main())
