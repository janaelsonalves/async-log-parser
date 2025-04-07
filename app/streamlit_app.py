import streamlit as st
import asyncio
import aiofiles
import tempfile
import os
import re
import pandas as pd
import altair as alt
from io import StringIO
import nest_asyncio

patterns = {
            'target_lines': re.compile(r'(Logged users|User Authenticated)'),
            'datetime': re.compile(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})'),
            'key_value': re.compile(r'([A-Za-z]+\.[A-Za-z-]+)=([^,\s\[\]]+)')
        }

# ------------------------------ CONFIG ------------------------------
# Expressão regular para capturar chave=valor
LOG_PATTERN = re.compile(r'(\w[\w.-]*)=([^,]+)')
# LOG_PATTERN = re.compile(r'(\d+-\d+-\d+\s\d+:\d+:\d+)|(\w[\w.-]*)=([^,]+)')

CHUNK_SIZE = 100

# -------------------------- PARSING LOGIC ---------------------------

async def process_lines(lines, filename):
    parsed = []
    for line in lines:
        if patterns['target_lines'].search(line):        
            log_entry = {}
            # Extrai data e hora
            if dt_match := patterns['datetime'].search(line):
                log_entry['Timestamp'] = dt_match.group(1)

            # Extrai todos os pares chave=valor
            matches = LOG_PATTERN.findall(line)  # Aplica regex na linha
            if matches:
                log_entry["RADIUS.Filename"] = os.path.basename(filename)  # Nome do arquivo
                for key, value in matches:
                    log_entry[key.strip()] = value.strip()  # Remove espaços em branco
                    if len(log_entry) > 1:  # Evita armazenar entradas vazias
                        parsed.append(log_entry)  # Adiciona à lista
            await asyncio.sleep(0)
    return parsed

async def process_file_async(path, filename, chunk_size=CHUNK_SIZE):
    tasks = []
    buffer = []

    async with aiofiles.open(path, mode='r') as f:
        async for line in f:
            buffer.append(line)
            if len(buffer) >= chunk_size:
                tasks.append(process_lines(buffer.copy(), filename))
                buffer.clear()
        if buffer:
            tasks.append(process_lines(buffer, filename))

    results = await asyncio.gather(*tasks)

    all_logs = []
    for parsed in results:
        all_logs.extend(parsed)

    return all_logs

def run_batch_processing(filepaths_and_names):
    tasks = [
        process_file_async(path, name)
        for path, name in filepaths_and_names
    ]
    
    nest_asyncio.apply()
    return asyncio.run(asyncio.gather(*tasks))

# ---------------------------- UI APP -------------------------------

st.set_page_config(page_title="Async Log Parser", layout="wide")
st.title("📄 Async Log File Parser")

with st.expander("Clique para abrir e carregar arquivos", expanded=True):
    uploaded_files = st.file_uploader(
        "Upload one or more .log or .txt files", type=["log", "txt"], accept_multiple_files=True
    )

if uploaded_files:
    temp_files = []
    for uploaded_file in uploaded_files:
        with tempfile.NamedTemporaryFile(delete=False, mode='wb') as tmp:
            tmp.write(uploaded_file.read())
            temp_files.append((tmp.name, uploaded_file.name))

    if st.button("🚀 Process Uploaded Files"):
        with st.spinner("Processing asynchronously..."):
            results = run_batch_processing(temp_files)

        all_logs = []
        for (logs), (_, filename) in zip(results, temp_files):
            all_logs.extend(logs)

        # Cleanup temp files
        for path, _ in temp_files:
            os.remove(path)

        # Logs DataFrame
        df_logs = pd.DataFrame(all_logs)

        df_logs = df_logs.sort_values('Common.Username', ascending=False)

        df_logs = df_logs.drop_duplicates(subset='Common.Username', keep='first')

        # Table
        st.subheader(f"🧾 Registros processados ({len(df_logs)})")
        st.dataframe(df_logs)

        # Chart
        # st.subheader("📈 Logs Over Time")
        # chart = alt.Chart(df_logs).mark_line().encode(
        #     x='timestamp:T',
        #     y='count():Q',
        #     color='level:N'
        # ).interactive()
        # st.altair_chart(chart, use_container_width=True)

        # Export Button
        st.subheader("📥 Export Logs as CSV")
        csv_buffer = StringIO()
        df_logs.to_csv(csv_buffer, index=False)
        st.download_button(
            label="Download Parsed Logs",
            data=csv_buffer.getvalue(),
            file_name="parsed_logs.csv",
            mime="text/csv"
        )
