import requests
import xml.etree.ElementTree as ET

r = requests.get('https://cbr.ru/scripts/xml_daily.asp?date_req=05/12/2021')
root = ET.fromstring(r.text)

codes = []

for valute in root.findall('Valute'):
  char_code = valute.find('CharCode').text
  value = valute.find('Value').text.replace(',', '.')
  nominal = valute.find('Nominal').text
  
  codes.append(char_code)
  
print(codes)