import re
import asyncio

class RadiusLogFilter:
    def __init__(self):
        # Padrões regex otimizados
        self.patterns = {
            'target_lines': re.compile(r'(Logged users|User Authenticated)'),
            'datetime': re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})'),
            'key_value': re.compile(r'([A-Za-z]+\.[A-Za-z-]+)=([^,\s\[\]]+)')
        }
    
    async def process_line(self, line):
        """Processa uma linha individual"""
        if not self.patterns['target_lines'].search(line):
            return None
        
        result = {}
        
        # Extrai data/hora
        if dt_match := self.patterns['datetime'].search(line):
            result['Timestamp'] = dt_match.group(1)
        
        # Extrai todos os pares chave=valor
        for match in self.patterns['key_value'].finditer(line):
            key, value = match.groups()
            result[key] = value
        
        return result

    async def process_logs(self, log_data):
        """Processa múltiplas linhas de log"""
        results = []
        
        for line in log_data.split('\n'):
            if not line.strip():
                continue
                
            try:
                parsed = await self.process_line(line)
                if parsed:
                    results.append(parsed)
            except Exception as e:
                continue
            
            await asyncio.sleep(0)  # Ponto de yield para async
            
        return results

# Exemplo de uso:
async def main():
    # Seus logs de exemplo (substitua pela leitura de arquivo real)
    log_data = """
    Jan  1 00:51:27 10.58.0.129 2025-01-01 00:51:27,790 10.58.0.1 System Events 6967 1 0 Timestamp=Jan 01 2025 00:50:11.062 BRT,Component=RADIUS,Level=ERROR,Category=Authentication,Action=Unknown,Description=Failed to decode RADIUS packet - Received Accounting-Request packet from 10.235.4.2 with invalid signature!  (Shared secret is incorrect.)
    Jan  1 01:01:27 10.58.0.129 2025-01-01 01:01:27,799 10.58.0.1 System Events 6968 1 0 Timestamp=Jan 01 2025 01:00:11.044 BRT,Component=RADIUS,Level=ERROR,Category=Authentication,Action=Unknown,Description=Failed to decode RADIUS packet - Received Accounting-Request packet from 10.235.4.2 with invalid signature!  (Shared secret is incorrect.)
    Jan  1 01:01:27 10.58.0.129 2025-01-01 01:01:27,799 10.58.0.1 System Events 6969 1 0 Timestamp=Jan 01 2025 01:00:17.036 BRT,Component=RADIUS,Level=ERROR,Category=Authentication,Action=Unknown,Description=Failed to decode RADIUS packet - Received Accounting-Request packet from 10.235.4.2 with invalid signature!  (Shared secret is incorrect.)
    Jan  1 01:02:37 10.58.0.129 2025-01-01 01:02:37,38 10.58.0.1 Radius Accounting 365957 1 0 RADIUS.Acct-Username=diogosantana@mpf.mp.br,RADIUS.Acct-NAS-IP-Address=10.235.8.83,RADIUS.Acct-NAS-Port=0,RADIUS.Acct-NAS-Port-Type=Wireless-802.11,RADIUS.Acct-Calling-Station-Id=70d8c2478821,RADIUS.Acct-Framed-IP-Address=10.235.8.148,RADIUS.Acct-Session-Id=50E4E0B66070-70D8C2478821-67744800-B72CD,RADIUS.Acct-Session-Time=30223,RADIUS.Acct-Output-Pkts=122604,RADIUS.Acct-Input-Pkts=222923,RADIUS.Acct-Output-Octets=23874462,RADIUS.Acct-Input-Octets=44538069,RADIUS.Acct-Service-Name=Login-User,RADIUS.Acct-UpdatedAt=2025-01-01 01:01:20.516831-03
    Jan  1 01:02:37 10.58.0.129 2025-01-01 01:02:37,38 10.58.0.1 Radius Accounting 365958 1 0 RADIUS.Acct-Username=diogosantana@mpf.mp.br,RADIUS.Acct-NAS-IP-Address=10.235.8.83,RADIUS.Acct-NAS-Port=0,RADIUS.Acct-NAS-Port-Type=Wireless-802.11,RADIUS.Acct-Calling-Station-Id=70d8c2478821,RADIUS.Acct-Framed-IP-Address=10.235.8.148,RADIUS.Acct-Session-Id=50E4E0B66070-70D8C2478821-6774BE14-CC07,RADIUS.Acct-Service-Name=Login-User,RADIUS.Acct-UpdatedAt=2025-01-01 01:01:24.52419-03
    Jan  1 01:02:37 10.58.0.129 2025-01-01 01:02:37,38 10.58.0.1 Radius Accounting 365959 1 0 RADIUS.Acct-Username=diogosantana@mpf.mp.br,RADIUS.Acct-NAS-IP-Address=10.235.8.83,RADIUS.Acct-NAS-Port=0,RADIUS.Acct-NAS-Port-Type=Wireless-802.11,RADIUS.Acct-Calling-Station-Id=70d8c2478821,RADIUS.Acct-Framed-IP-Address=10.235.8.148,RADIUS.Acct-Session-Id=50E4E0B66070-70D8C2478821-6774BE14-CC07,RADIUS.Acct-Session-Time=0,RADIUS.Acct-Output-Pkts=2,RADIUS.Acct-Input-Pkts=2,RADIUS.Acct-Output-Octets=482,RADIUS.Acct-Input-Octets=374,RADIUS.Acct-Service-Name=Login-User,RADIUS.Acct-UpdatedAt=2025-01-01 01:01:24.52419-03
    Jan  1 01:02:37 10.58.0.129 2025-01-01 01:02:37,38 10.58.0.1 Radius Accounting 365960 1 0 RADIUS.Acct-Username=diogosantana@mpf.mp.br,RADIUS.Acct-NAS-IP-Address=10.235.8.83,RADIUS.Acct-NAS-Port=0,RADIUS.Acct-NAS-Port-Type=Wireless-802.11,RADIUS.Acct-Calling-Station-Id=70d8c2478821,RADIUS.Acct-Framed-IP-Address=10.235.8.148,RADIUS.Acct-Session-Id=50E4E0B66060-70D8C2478821-6774BE15-80AFB,RADIUS.Acct-Service-Name=Login-User,RADIUS.Acct-UpdatedAt=2025-01-01 01:01:26.529686-03
    Jan  1 01:02:37 10.58.0.129 2025-01-01 01:02:37,38 10.58.0.1 Radius Accounting 365961 1 0 RADIUS.Acct-Username=diogosantana@mpf.mp.br,RADIUS.Acct-NAS-IP-Address=10.235.8.83,RADIUS.Acct-NAS-Port=0,RADIUS.Acct-NAS-Port-Type=Wireless-802.11,RADIUS.Acct-Calling-Station-Id=70d8c2478821,RADIUS.Acct-Framed-IP-Address=10.235.8.148,RADIUS.Acct-Session-Id=50E4E0B66060-70D8C2478821-6774BE15-80AFB,RADIUS.Acct-Session-Time=20,RADIUS.Acct-Output-Pkts=163,RADIUS.Acct-Input-Pkts=245,RADIUS.Acct-Output-Octets=43584,RADIUS.Acct-Input-Octets=64641,RADIUS.Acct-Service-Name=Login-User,RADIUS.Acct-UpdatedAt=2025-01-01 01:01:46.569554-03
    Jan  1 01:02:37 10.58.0.129 2025-01-01 01:02:37,38 10.58.0.1 Radius Accounting 365962 1 0 RADIUS.Acct-Username=diogosantana@mpf.mp.br,RADIUS.Acct-NAS-IP-Address=10.235.8.83,RADIUS.Acct-NAS-Port=0,RADIUS.Acct-NAS-Port-Type=Wireless-802.11,RADIUS.Acct-Calling-Station-Id=70d8c2478821,RADIUS.Acct-Framed-IP-Address=10.235.8.148,RADIUS.Acct-Session-Id=50E4E0B66070-70D8C2478821-6774BE2A-5BFEF,RADIUS.Acct-Service-Name=Login-User,RADIUS.Acct-UpdatedAt=2025-01-01 01:01:46.569554-03
    Jan  1 01:02:48 10.58.0.129 2025-01-01 01:02:48,577 10.58.0.1 Logged users 57645 1 0 Common.Username=diogosantana@mpf.mp.br,Common.Service=MPF-Institucional Aruba 802.1X Wireless,Common.Roles=SERVIDORES, [User Authenticated],Common.Host-MAC-Address=70d8c2478821,RADIUS.Acct-Framed-IP-Address=10.235.8.148,Common.NAS-IP-Address=10.235.8.83,Common.Request-UpdatedAt=2025-01-01 01:01:25.484332-03
    Jan  1 01:02:48 10.58.0.129 2025-01-01 01:02:48,577 10.58.0.1 Logged users 57646 1 0 Common.Username=diogosantana@mpf.mp.br,Common.Service=MPF-Institucional Aruba 802.1X Wireless,Common.Roles=SERVIDORES, [User Authenticated],Common.Host-MAC-Address=70d8c2478821,RADIUS.Acct-Framed-IP-Address=10.235.8.148,Common.NAS-IP-Address=10.235.8.83,Common.Request-UpdatedAt=2025-01-01 01:01:25.484332-03
    Jan  1 01:02:48 10.58.0.129 2025-01-01 01:02:48,577 10.58.0.1 Logged users 57647 1 0 Common.Username=diogosantana@mpf.mp.br,Common.Service=MPF-Institucional Aruba 802.1X Wireless,Common.Roles=SERVIDORES, [User Authenticated],Common.Host-MAC-Address=70d8c2478821,RADIUS.Acct-Framed-IP-Address=10.235.8.148,Common.NAS-IP-Address=10.235.8.83,Common.Request-UpdatedAt=2025-01-01 01:01:25.484332-03
    Jan  1 01:02:48 10.58.0.129 2025-01-01 01:02:48,577 10.58.0.1 Logged users 57648 1 0 Common.Username=diogosantana@mpf.mp.br,Common.Service=MPF-Institucional Aruba 802.1X Wireless,Common.Roles=SERVIDORES, [User Authenticated],Common.Host-MAC-Address=70d8c2478821,RADIUS.Acct-Framed-IP-Address=10.235.8.148,Common.NAS-IP-Address=10.235.8.83,Common.Request-UpdatedAt=2025-01-01 01:01:25.484332-03
    Jan  1 01:02:48 10.58.0.129 2025-01-01 01:02:48,577 10.58.0.1 Logged users 57649 1 0 Common.Username=diogosantana@mpf.mp.br,Common.Service=MPF-Institucional Aruba 802.1X Wireless,Common.Roles=SERVIDORES, [User Authenticated],Common.Host-MAC-Address=70d8c2478821,RADIUS.Acct-Framed-IP-Address=10.235.8.148,Common.NAS-IP-Address=10.235.8.83,Common.Request-UpdatedAt=2025-01-01 01:01:25.484332-03
    Jan  1 01:05:27 10.58.0.129 2025-01-01 01:05:27,803 10.58.0.1 System Events 6970 1 0 Timestamp=Jan 01 2025 01:05:11.056 BRT,Component=RADIUS,Level=ERROR,Category=Authentication,Action=Unknown,Description=Failed to decode RADIUS packet - Received Accounting-Request packet from 10.235.4.2 with invalid signature!  (Shared secret is incorrect.)
    """
    
    parser = RadiusLogFilter()
    filtered_data = await parser.process_logs(log_data)
    
    print("\nExemplo de entrada:")
    print(filtered_data)
    print(len(filtered_data))

if __name__ == "__main__":
    asyncio.run(main())