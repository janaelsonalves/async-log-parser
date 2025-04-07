import re
import asyncio
import aiofiles
from collections import Counter

# --- CONFIG ------------------------------------------------------------------

LOG_PATTERN = re.compile(r'(\w[\w.-]*)=([^,]+)')

CHUNK_SIZE = 100

# --- LOGIC: Parse chunk of lines ---------------------------------------------

async def process_lines(lines):
    parsed = []
    counter = Counter()

    for line in lines:
        match = LOG_PATTERN.match(line)
        if match:
            data = match.groupdict()
            parsed.append(data)
            counter[data["level"]] += 1
        await asyncio.sleep(0)  # allow event loop to yield

    return parsed, counter

# --- LOGIC: Main async processor using gather --------------------------------

async def process_log_file_async(path, chunk_size=CHUNK_SIZE):
    tasks = []
    buffer = []

    async with aiofiles.open(path, mode='r') as f:
        async for line in f:
            buffer.append(line)
            if len(buffer) >= chunk_size:
                tasks.append(process_lines(buffer.copy()))
                buffer.clear()

        if buffer:
            tasks.append(process_lines(buffer))

    results = await asyncio.gather(*tasks)

    logs, counter = [], Counter()
    for parsed, c in results:
        logs.extend(parsed)
        counter.update(c)

    return logs, counter

# --- WRAPPER for asyncio.run() -----------------------------------------------

def run_processing(path):
    return asyncio.run(process_log_file_async(path))