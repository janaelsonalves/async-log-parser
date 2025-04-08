import streamlit as st
from streamlit_dynamic_filters import DynamicFilters
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
            'target_lines': re.compile(r'Radius Accounting'),
            'datetime': re.compile(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})'),
            'key_value': re.compile(r'([A-Za-z]+\.[A-Za-z-]+)=([^,\s\[\]]+)')
            # 'key_value': re.compile(r'(\w[\w.-]*)=([^,]+)')
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
        # if re.search(r'10.65', line):
            log_entry = {}
            # Extrai data e hora
            dt_match = patterns['datetime'].search(line)
            log_entry['RADIUS.Timestamp'] = dt_match.group(1)

            # Extrai todos os pares chave=valor
            # matches = LOG_PATTERN.findall(line)
            matches = patterns['key_value'].findall(line)  # Aplica regex na linha
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
        process_file_async(path, name) for path, name in filepaths_and_names
    ]
    
    nest_asyncio.apply()
    return asyncio.run(asyncio.gather(*tasks))

@st.cache_data
def prepare_dataframe(df_logs):
    try:        
        df_logs = df_logs.sort_values(['RADIUS.Timestamp', 'RADIUS.Acct-Username'], ascending=False)
        df_logs = df_logs.drop_duplicates(subset='RADIUS.Acct-Username', keep='first')
        df_logs = df_logs[df_logs['RADIUS.Acct-NAS-IP-Address'].str.contains('10.65')]
    except Exception as e:
        print(e)
    return df_logs

def create_tempfiles(uploaded_files):
    temp_files = []
    for uploaded_file in uploaded_files:
        with tempfile.NamedTemporaryFile(delete=False, mode='wb') as tmp:
            tmp.write(uploaded_file.read())
            temp_files.append((tmp.name, uploaded_file.name))
    return temp_files

def remove_tempfiles(temp_files):
    for path, _ in temp_files:
        os.remove(path)

def extend_tempfiles_list(temp_files, results):
    all_logs = []
    # for (logs), (_, filename) in zip(results, temp_files):
    for logs, _ in zip(results, temp_files):
        all_logs.extend(logs)
    return all_logs

# ---------------------------- UI APP -------------------------------
st.set_page_config(page_title="Analisador de logs do CPPM (Radius)", layout="wide")
st.title("📄 Analisador de logs do CPPM (Radius)")

with st.expander("## Carregue arquivos em formato .log ou .txt", expanded=True, icon="📋") as exp:
    uploaded_files = st.file_uploader("Upload log files", type=["log", "txt"], accept_multiple_files=True, label_visibility='hidden')

if uploaded_files:
    temp_files = create_tempfiles(uploaded_files)

    if st.button("🚀 Excluir arquivos temporários"):
        # Cleanup temp files
        # remove_tempfiles(temp_files)
        # st.file_uploader = []
        pass

    disabled = False
    if st.button("🚀 Processasr arquivos carregados", disabled=False, key="active"):
        with st.spinner("Processando arquivos... Aguarde!"):
            from datetime import datetime
            inicial = datetime.now()
            results = run_batch_processing(temp_files)
            final = datetime.now()
            disabled = True

        st.write(f"**Tempo de processamento:** _{final - inicial}_")

        all_logs = extend_tempfiles_list(temp_files, results)
        
        # Logs DataFrame
        df_logs = pd.DataFrame(all_logs)
        # df_logs = prepare_dataframe(df_logs)
        df_logs = df_logs.sort_values(['RADIUS.Timestamp', 'RADIUS.Acct-Username'], ascending=False)
        df_logs = df_logs.drop_duplicates(subset='RADIUS.Acct-Username', keep='first')
        df_logs = df_logs[df_logs['RADIUS.Acct-NAS-IP-Address'].str.contains('10.65')]

        remove_tempfiles(temp_files)

        # Table
        st.subheader(f"🧾 Registros processados ({len(df_logs)})")

        if len(df_logs):
            st.dataframe(df_logs[['RADIUS.Filename', 'RADIUS.Timestamp', 'RADIUS.Acct-Username', 'RADIUS.Acct-NAS-IP-Address', 'RADIUS.Acct-Framed-IP-Address', 'RADIUS.Acct-NAS-Port-Type']]) 
            # st.dataframe(df_logs)

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
