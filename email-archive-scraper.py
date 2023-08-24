#!python3
from bs4 import BeautifulSoup
from datetime import datetime
import requests
from typing import List, Optional
import re
import asyncio
import hashlib
from aiohttp import ClientSession
from typing import Any, Union
import logging
import concurrent.futures
from dateutil.parser import parse as parse_date
import email
import mimetypes
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from math import ceil, floor
from requests.exceptions import HTTPError
import argparse

logger = logging.getLogger('archive-scraper')
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)



class Header(dict):
    def __init__(self, subject, name, email, reply, date):
        super().__init__(
            subject=subject,
            from_name=name,
            from_email=email,
            reply=reply,
            date=date.isoformat()
        )

class Message(dict):
    def __init__(self, url, header, body):
        super().__init__(
            url=url,
            header=header,
            body=body
        )

async def read_message(url) -> Message:

    try:
        response = requests.get(url)
        response.raise_for_status()
        response_body = response.text
        dom = BeautifulSoup(response_body, 'html.parser')

        header = Header(
            subject=dom.select('html body h1').pop(0).text.strip(),
            name=dom.select('html body b').pop(0).text.strip(),
            email=dom.select('html body a').pop(0).text.strip().replace(' at ', '@'),
            reply=dom.select('html body a').pop(0).get('href') or '',
            # date=datetime.strptime(dom.select('html body i').pop(0).text.strip(), '%Y-%m-%d %H:%M:%S')
            date=parse_date(dom.select('html body i').pop(0).text.strip())
        )
        body = dom.select('html body p pre').pop(0).text.strip()
        logger.info(f'read message at url:{url}')
        return Message(
            url=url,
            header=header,
            body=body
        )

    except ValueError as e:
        logger.error(f'error on url {url}')
        logger.error(e)
    except AttributeError as e:
        logger.error(f'error on url {url}')
        logger.error(e)
    except Exception as e:
        logger.error(f'error on url {url}')
        logger.exception(e)
        raise e

async def read_month(url: str) -> List[str]:
    url = url.rstrip('/')
    response = requests.get(url + '/date.html')
    response.raise_for_status()
    body = response.text

    urls = set()
    pattern = '\"(\d+\.html)\"'
    matches = re.findall(pattern, body)
    for match in matches:
        urls.add(url + '/' + match)

    logger.info(f'Found {len(urls)} message urls for month at {url.split("/")[-1]}')

    return list(urls)

async def save_month_emls(url:str):
    pass

async def read_index(url: str, options: Optional[dict] = None) -> List[str]:
    url = url.rstrip('/')
    response = requests.get(url)
    response.raise_for_status()
    body = response.text

    # pattern = options.get('regex', r'\d{4}-[a-zA-Z]+\.txt(?:\.gz)?')
    pattern = options.get('regex', r'\d{4}-[a-zA-Z]+')
    matches = re.findall(pattern, body)
    # urls = [f"{url}/{match.replace('.txt', '').replace('.gz', '')}" for match in matches]
    
    ordered_unique_urls = []
    
    for match in matches:
        full_url = f"{url}/{match}"
        if full_url not in ordered_unique_urls:
            ordered_unique_urls.append(full_url)

    logger.info('Finished reading index')
    return list(reversed(ordered_unique_urls))

#TODO: break out index retrieval
async def download_emails_from_links(src: str, options: dict = {}) -> asyncio.Queue:

    logging.info('fetching month urls')
    months = await read_index(src, options)
        
    if 'months' in options and options['months'] != float('inf') and options['months'] is not None:
        if 'start_month' in options and options['start_month'] != float('inf') and options['start_month'] is not None:
            months = slice_from_args(options['start_month'], options['months'], months)
        else:
            months = months[-options['months']:]

    # month_reads = [read_month(month) for month in months if filter_month(month)]
    concurrent_limit = 4
    semaphore = asyncio.Semaphore(concurrent_limit)
    
    async def safe_read_month(url):
        async with semaphore:
            return await read_month(url)
            
    async def safe_read_message(url):
        async with semaphore:
            return await read_message(url)

    nested_urls = await asyncio.gather(*[safe_read_month(month) for month in months])
    logger.info('collected message urls')
    
    message_urls = [url for sublist in nested_urls for url in sublist]
    messages = await asyncio.gather(*[safe_read_message(url) for url in message_urls])
    
    return messages

def TRUE(a: Any) -> bool:
    return True

def create_mime_message(message: Message) -> email.message.Message:
    mime_message = MIMEMultipart()
    mime_message["From"] = f"{message['header']['from_name']} <{message['header']['from_email']}>"
    mime_message["To"] = message['header']["reply"]
    mime_message["Subject"] = message['header']["subject"]
    mime_message["Date"] = message['header']["date"]

    body = MIMEText(message['body'])
    mime_message.attach(body)

    return mime_message

async def download_month_archive_list(url: str) -> List[str]:
    url = url.rstrip('/')
    try:
        response = requests.get(url + '.txt')
        body = response.text
        logger.info(f'Finished downloading month archive at {url.split("/")[-1]}')
    except HTTPError as http_error:
        logger.error(f' An HTTP Error ocurred while fetching {url}')
        logger.error(f' "{http_error.strerror}" ')
        return []
    #extract messages from text into list
    regex_pattern = r"^From\s+\S+\s+(at)\s"
    kept_messages = []
    recombined_body = ''
    lines = body.splitlines()
    first_line = True

    for line in lines:
        match = re.match(regex_pattern, line)
        if not match:
            if first_line:
                line = line.replace(' at ', '@')
                first_line = False
            recombined_body += line + '\n'
        else:
            kept_messages.append(recombined_body)
            recombined_body = ''
            first_line = True
    if recombined_body != '':
        kept_messages.append(recombined_body)
    logger.info(f'Downloaded {len(kept_messages)} messages from {url.split("/")[-1]}')
    return list(kept_messages)

def slice_from_args(start_month:int, num_months:int, months):
    i = max(start_month-1,0)  # the lowest valid index for an array is i=0
    j = min(i+num_months,len(months))  # the highest valid index for an array is len(arr)-1
    return months[i:j]

async def download_email_text_archives(src: str, options: Optional[dict] = None) -> List[str]:
    filter_month = options.get('filterMonth', TRUE)

    #get all month links from the main page
    months = await read_index(src, options)
    
    # ensure we have sane values
    if 'months' in options and options['months'] != float('inf') and options['months'] is not None:
        if 'start_month' in options and options['start_month'] != float('inf') and options['start_month'] is not None:
            months = slice_from_args(options['start_month'], options['months'], months)
        else:
            months = months[-options['months']:]
    
    # limit the async to 4 coroutines to avoid rate limiting
    concurrent_limit = 4
    semaphore = asyncio.Semaphore(concurrent_limit)
    async def safe_download_month_archive_list(url):
        async with semaphore:
           return await download_month_archive_list(url)

    logger.info(f'Fetching text archives from {src}')
    text_archives = await asyncio.gather(*[safe_download_month_archive_list(month) for month in months if filter_month(month)])
    messages = [message 
                for month_archive in text_archives 
                for message in month_archive
                if message != '']
    logger.info('Finished downloading messages')
    return messages

def create_eml_content(message: Message) -> str:
    eml_content = f"From: {message['header']['from_name']} <{message['header']['from_email']}>\n"
    eml_content += f"Subject: {message['header']['subject']}\n"
    eml_content += f"Date: {message['header']['date']}\n"
    eml_content += "\n"  # Add an empty line to separate headers from body
    eml_content += message['body']
    return eml_content
            
def save_message_list_as_eml_files(messages: list[Message], output_folder: str):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        
    logger.info(f'Writing {len(messages)} messages to {output_folder}')

    for idx, message in enumerate(messages, start=1):
        eml_content = create_eml_content(message)
        hash_result = hashlib.blake2b(bytes(eml_content,'utf-8'), digest_size=8)
        eml_file_path = os.path.join(output_folder, f"message_{hash_result.hexdigest()}.eml")
        with open(eml_file_path, "w") as eml_file:
            eml_file.write(eml_content)


async def main():
    src = "https://web.archive.org/web/20230128202117/http://lists.extropy.org/pipermail/exi-east/"
    # messages = await download(src, options)
    # month_start = 0
    # num_months = 12
    # output_file = "eml_files"
    # save_messages_as_eml(messages, output_file)

    parser = argparse.ArgumentParser(description="Extract emails for a given period")
    parser.add_argument("root_url", type=str, help="Root URL for the archive")
    parser.add_argument("-s","--start-month", type=int, default=1, help="Starting month")
    parser.add_argument("-m","--months", type=int, default=None, help="Number of months")
    parser.add_argument("-o","--output-folder", help="Output folder path", default='')

    args = parser.parse_args()
    if args.output_folder == '':
        args.output_folder == f'{args.start_month}-{args.months}'
    # extract_emails(, args.months, args.output_folder)
    
    options = {
        'start_month': args.start_month,
        'months': args.months,
        'output_folder':args.output_folder
        }
    
    logger.info('beginning download process')
    messages = await download_emails_from_links(args.root_url, options)
    # messages = await download_email_text_archives(args.root_url, options)
    
    save_message_list_as_eml_files(messages, args.output_folder)

    # create_mbox_file(messages,'all_emails.mbox')

if __name__ == "__main__":
    asyncio.run(main())

