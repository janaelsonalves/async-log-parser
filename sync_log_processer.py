import os
import re
import pandas as pd
from multiprocessing import Pool, cpu_count

# Expressões regulares
patterns = {
    'target_lines': re.compile(r'Login-User'),
    # 'ip_filter': re.compile(r'\b10\.65\.(?:\d{1,3})\.(?:\d{1,3})\b'),
    'datetime': re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'),
    'ip_filter': re.compile(r'10\.65\.\d+\.\d+'),
    # 'key_value': re.compile(r'([A-Za-z]+\.[A-Za-z-]+)=([^,\s\[\]]+)')
    'key_value': re.compile(r'(\w[\w.-]*)=([^,]+)')
}

# Extrai dados de uma linha
def extract_data_from_line(line):
    if not patterns['target_lines'].search(line): #and patterns['ip_filter'].search(line)):
        return None

    try:
        log_entry = {}

        # Extrai data/hora
        dt_match = patterns['datetime'].search(line)

        log_entry['RADIUS.Timestamp'] = dt_match.group(1)

        # Extrai pares chave=valor
        matches = patterns['key_value'].findall(line)
        if matches:
            # log_entry["RADIUS.Filename"] = os.path.basename(filename)
            for key, value in matches:
                log_entry[key.strip()] = value.strip()

                    # parsed.append(log_entry)  # Apenas 1 append por linha válida
        return log_entry
    except Exception as e:
        print(e)
        return None

# Processa um único arquivo
def process_file(file_path):
    extracted = []
    with open(file_path, 'r', encoding='utf-8') as f:
        file_name = os.path.basename(file_path)
        print(f"Processando arquivo: {file_name}")
        for line in f:
            row = extract_data_from_line(line)
            if row:
                row["RADIUS.Filename"] = file_name
                extracted.append(row)
    return extracted

# Processa todos os arquivos com Pool
def process_all_logs(log_dir):
    files = [os.path.join(log_dir, f) for f in os.listdir(log_dir) if f.endswith('.log')]
    with Pool(cpu_count()) as pool:
        results = pool.map(process_file, files)

    # Achata a lista de listas
    all_rows = [row for sublist in results for row in sublist]
    return pd.DataFrame(all_rows)

# Exemplo de uso
if __name__ == "__main__":
    logdir = "C:/Users/Janaelson/Downloads/LogsClearPass"
    # df = process_all_logs("logs")
    df = process_all_logs(logdir)
    df = df.drop_duplicates(subset=['RADIUS.Acct-Username'], keep='first')
    df = df[['RADIUS.Timestamp','RADIUS.Filename','RADIUS.Acct-Username', 'RADIUS.Acct-Calling-Station-Id', 'RADIUS.Acct-Framed-IP-Address', 'RADIUS.Acct-NAS-IP-Address', 'RADIUS.Acct-Service-Name', 'Common.Auth-Type']]
    df.to_csv("output_multiprocess.csv", index=False)
    print("Arquivo salvo: output_multiprocess.csv")
