import requests, re, json, csv
from datetime import datetime
from bs4 import BeautifulSoup
from requests import RequestException
from requests.exceptions import HTTPError
from requests.exceptions import InvalidURL

    
def netflix_scraper(url):
  try:
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
  except RequestException as e:
    print(e)
  else:
    links = []
    
    first_batch = soup.script.text
    json_format = json.loads(first_batch)
    pees=json_format["itemListElement"]
    for pee in pees:
      links.append(pee["item"]['url'])
    
    for link in links:
      try:
        r = requests.get(link)
        second = BeautifulSoup(r.text, 'html.parser')
      except RequestException as e:
        print(e)
      else:
        movies = second.script.text
        formatted = json.loads(movies)
        
        typ = formatted["@type"]
        url = formatted["url"]
        name = formatted["name"]
        rating = formatted["contentRating"]
        description = formatted["description"]
        genre = formatted["genre"]
        image = formatted["image"]
        dateCreated = formatted["dateCreated"]
        actors = [actor["name"] for actor in formatted["actors"]]
        director = [direct["name"] for direct in formatted["director"]]
        creator = [creat["name"] for creat in formatted["creator"]]
        
        movie = {
          "Name": name,
          "Type": typ,
          "URL": url,
          "Rating": rating,
          "Date_Created": dateCreated,
          "Image": image,
          "Genre": genre,
          "Description": description,
          "Actors": actors,
          "Director": director,
          "Creator": creator
        }
        with open("netflix_movies.json", "a") as file:
          json.dump(movie, file)
        
        # writer.writerow([name,genre,typ, dateCreated,rating,description,image])
      #return "Done!!!"
          
    

    
if __name__=="__main__":
  print(netflix_scraper("https://www.netflix.com/ng/browse/genre/34399"))
  
