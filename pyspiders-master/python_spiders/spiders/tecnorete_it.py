# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class TecnoreteSpider(Spider):
    name = 'Tecnorete_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.tecnorete.it"]
    cities_provinace_dictionary = {}
    total_urls = []

    def start_requests(self):
            
            for letter in range(97, 123):
                    search_letter = chr(letter)
                    yield Request(url=f'https://www.tecnorete.it/api/geo/autocomplete?section=estate&search={search_letter}',
                      callback=self.parse,
                      body='',
                      method='GET')


    def parse(self, response):
        try:
            cities = response.json()["cities"]
            for city in cities:
                    city_name = city["name"]
                    province= city["province_id"]
                    self.cities_provinace_dictionary[province] = city_name
            self.total_urls.append(response.url)

            if(len(self.total_urls) == 23):
                for province, city_name in self.cities_provinace_dictionary.items():
                    yield Request(
                                url=f"https://www.tecnorete.it/api/estates/search?contract=locazi&placeholder={city_name}&province={province}&sector=res&type=&section=estate", 
                                callback=self.get_estates, 
                                dont_filter=True)        

        except TypeError:
            pass
        except KeyError:
            pass


        
    def get_estates(self, response):
        estates = response.json()["estates"]
        for estate in estates:
            item_loader = ListingLoader(response=response)
            
            title = estate["title"]
            if (("commerciale" in title.lower()) or ("ufficio" in title.lower()) or ("magazzino" in title.lower()) or ("box" in title.lower()) ):
                continue

            external_link = estate["detail_url"]
            property_type = "apartment"
            rent = estate["price"].split(" / ")[0]
            address = estate["subtitle"]
            room_count = estate["rooms"].split(" locali")[0]
            square_meters = estate["surface"]
            bathroom_count = estate["bathrooms"].split(" bagn")[0]

            images = []
            for image in estate["images"]:
                images.append(image["url"])

            item_loader.add_value("external_link", external_link)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("rent_string", rent)
            item_loader.add_value("address", address)
            item_loader.add_value("title", title)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
        
            yield item_loader.load_item()
