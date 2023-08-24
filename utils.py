def TRUE(a: Any) -> bool:
    return True

def read_message(url):
    response = requests.get(url)
    response.raise_for_status()
    body = response.text

    try:
        dom = BeautifulSoup(body, 'html.parser')

        header = Header(
            subject=dom.select('html body h1').pop(0).text.strip(),
            name=dom.select('html body b').pop(0).text.strip(),
            email=dom.select('html body a').pop(0).text.strip().replace(' at ', '@'),
            reply=dom.select('html body a').pop(0).get('href') or '',
            date=datetime.strptime(dom.select('html body i').pop(0).text.strip(), '%Y-%m-%d %H:%M:%S')
        )

        return Message(
            url=url,
            header=header,
            body=dom.select('html body p pre').pop(0).text.strip()
        )
    except Exception as ex:
        ex.message += '\n\n\n' + body
        raise ex

async def read_index(url: str, options: Optional[dict] = None) -> List[str]:
    url = url.rstrip('/')
    response = requests.get(url)
    response.raise_for_status()
    body = response.text

    pattern = options.get('archiveUrlRegex', r'\d{4}-[a-zA-Z]+\.txt(?:\.gz)?')
    matches = re.findall(pattern, body)
    urls = [f"{url}/{match.replace('.txt', '').replace('.gz', '')}" for match in matches]

    return list(reversed(urls))

async def read_month(url: str) -> List[str]:
    url = url.rstrip('/')
    response = requests.get(url + '/date.html')
    response.raise_for_status()
    body = response.text
    
    info(f'fetching url {url}')

    urls = set()
    pattern = r'href=\"(\d+\.html)\"'
    matches = re.findall(pattern, body)
    for match in matches:
        urls.add(url + '/' + match)

    return list(urls)

async def download(src: str, options: dict = {}) -> asyncio.Queue:
    filter_month = options.get('filterMonth', TRUE)
    filter_message = options.get('filterMessage', TRUE)

    months = await read_index(src, options)
    if 'months' in options and options['months'] != float('inf'):
        months = months[-options['months']:]
    logging.info('reading months')
    # month_reads = [read_month(month) for month in months if filter_month(month)]
    
    queue = to_stream(asyncio.gather(*[read_month(month) for month in months if filter_month(month)]))
    # filtered = async for val in async_filter(lambda url: filter_message(url), queue)
    # filtered = asyncio.Queue()
    # messages = [read_message(url) for url in queue if filter_message(url)]
    queue = await queue
    messages = asyncio.Queue()
    
        
    while True:
        # get a unit of work without blocking
        try:
            url = queue.get_nowait()
            message = read_message(url)
            await messages.put(message)
        except asyncio.QueueEmpty:
            print('Consumer: got nothing, exiting..')
            break
        # check for stop
        if url is None:
            break
        # report
        print(f'>got {message}')