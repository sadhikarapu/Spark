from bs4 import BeautifulSoup
import requests

headers = {'User-Agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0"}
http_browser = requests.get("https://www.moneycontrol.com/india/stockpricequote/banks-private-sector/yesbank/YB",headers = headers)

soup = BeautifulSoup(http_browser.content,"lxml")

get_title_text = soup.find_all("title") 


data_split =  get_title_text[0].text.split(",")

print(data_split[5].split(".")[0])


