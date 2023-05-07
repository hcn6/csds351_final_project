import requests
from bs4 import BeautifulSoup
import re

def retrieve_html(url: str):
    response = requests.get(url)
    status = response.status_code

    if status != 200:
        return None
    
    return response.text

def create_soup_object(html: str):
    return BeautifulSoup(html, 'html.parser')

def get_all_cve_in_year(year: int):
    url = f"https://cve.mitre.org/cgi-bin/cvekey.cgi?keyword={year}"
    html = retrieve_html(url)
    # print(html)
    with open('test.html', 'w') as f:
        f.write(html)
    if html is None:
        return None
    soup = create_soup_object(html)
    parent_tag = soup.find('div', id='CenterPane')
    cve_tag = parent_tag.select('tr > td > a')
    cve_content = [tag.text for tag in cve_tag]
    
    pattern = r'CVE-2023-\d+'
    cve_content = [cve for cve in cve_content if re.match(pattern, cve)]

    return cve_content

def extract_info_from_one_cve(cve_id: str):
    url = f"https://nvd.nist.gov/vuln/detail/{cve_id}"
    html = retrieve_html(url)
    with open('test.html', 'w') as f:
        f.write(html)
    if html is None:
        return None
    soup = create_soup_object(html)
    parent_tag = soup.find('div', id='Vuln3CvssPanel')
    print(parent_tag)
    first_div = parent_tag.contents[0]
    severity = first_div.find('span', class_='severityDetail').text
    return severity

if __name__ == "__main__":
    cve_content = get_all_cve_in_year(2023)
    # severity_list = []
    # for cve in cve_content:
    #     severity_list.append(extract_info_from_one_cve(cve))
    
    # #zip cve_content and severity_list and map to dict
    # cve_severity = dict(zip(cve_content, severity_list))
    # print(cve_severity[:10])
    print(extract_info_from_one_cve(cve_content[0]))

# "/opt/homebrew/var/mongodb"
# mongod --port 27017 --dbpath "/opt/homebrew/var/mongodb" --replSet rs0 --bind_ip localhost

