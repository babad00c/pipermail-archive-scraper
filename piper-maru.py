

async def main():
    src = "https://web.archive.org/web/20230601054010/http://lists.extropy.org/pipermail/paleopsych/"
    options = {'months': 2}
    messages_queue = await download(src, options)

    async for message in messages_queue:
        print(message)

if __name__ == "__main__":
    asyncio.run(main())