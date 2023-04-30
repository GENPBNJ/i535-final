#%%
import requests
from bs4 import BeautifulSoup
from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.common.by import By
from selenium import webdriver
import feedparser
import pandas as pd



url = "https://trends.google.com/trends/trendingsearches/daily/rss?geo=US"

feed = feedparser.parse(url)

titles = []
height_approx_traffic = []
description = []
published = []

for entry in feed.entries:
    titles.append(entry.title)
    height_approx_traffic.append(entry.ht_approx_traffic)
    description.append(entry.description)
    published.append(entry.published)

data = {'Titles': titles, 'Height Traffic': height_approx_traffic, 'Description':description,'Published Date':published}
df = pd.DataFrame(data)

# %%
