import os 
import time
import requests
import multiprocessing 

from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String
from sqlalchemy import DateTime, Date
from sqlalchemy import Integer
from sqlalchemy import create_engine
from sqlalchemy import ForeignKey

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
connection = "sqlite:///" + os.path.join(BASE_DIR, "netflix.db")
engine = create_engine(connection, echo=True)

Base = declarative_base()

class Movie(Base):
  __tablename__="Movie"
  id = Column(Integer(), primary_key=True)
  title = Column(String())
  year = Column(Integer())
  rating = Column(String())
  duration = Column(DateTime())
  description = Column(String())
  
class Genre(Base):
  __tablename__="Genre"
  id = Column(Integer(), primary_key=True)
  name = Column(String())
  movie_id = Column(ForeignKey("Movie.id"))
  
class Stars(Base):
  __tablename__="Star"
  id = Column(Integer(), primary_key=True)
  name = Column(String())
  movie_id = Column(ForeignKey("Movie.id"))

#Base.metadata.create_all(engine)

def get_links(url, queue):
    #session = requests.Session()
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    links = []
    sections = soup.find_all('section', {'class': "nm-collections-row"})
    for section in sections:
        movies = section.find("div", {"class": "nm-content-horizontal-row"}).find_all("li", {"class": "nm-content-horizontal-row-item"})
        for movie in movies:
            try:
                link = movie.a.get("href")
                if link not in links:
                    print(link)
                    #print(movie_info(link))
                    queue.put(link)
            except:
                continue
    #return links

def movie_info(url):
    #session = requests.Session()
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    detail = soup.find('div', {"class": "title-info"})
    title = detail.h1.text
    col = detail.find('div', {"class": "title-info-metadata-wrapper"}).find_all("span")
    year = col[0].text
    maturity = col[4].text
    duration = col[7].text
    genre = detail.find('div', {"class": "title-info-metadata-wrapper"}).find("a", {"class": "title-info-metadata-item item-genre"}).text
    description = detail.find('div', {"class": "title-info-synopsis-talent"}).find("div", {"class": "title-info-synopsis"}).text
    stars = detail.find('div', {"class": "title-info-synopsis-talent"}).find("div", {"class": "title-info-talent"}).find_all("span")[1].text.split(", ")
    
    print(f"title: {title}\nyear: {year}\nmaturity: {maturity}\nduration: {duration}\ngenre: {genre}\ndescription: {description}\nstars: {stars}")
    
if __name__=="__main__":
  url = "https://www.netflix.com/ng/browse/genre/34399"
  
  queue = multiprocessing.Queue()
  #links = get_links(url)
  # with ThreadPoolExecutor() as p:
  #   p.map(movie_info, links)
  
  p = multiprocessing.Process(target=get_links, args=(url, queue))
  p.start()
  while queue:
    link = queue.get()
    q = multiprocessing.Process(target=movie_info, args=(link,))
    q.start()