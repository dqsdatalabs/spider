# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'avenirimmobilier34_fr'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = 'Avenirimmobilier34_PySpider_france'
    
    post_url = "https://www.avenir-immobilier-34.fr/fr/data_listing-location_formrecherchelocation.html"
    other_prop = ["2"]
    other_type = ["house"]
    
    def start_requests(self):
        formdata = {
            "loc": "location",
            "type[]": "appartement",
            "surfacemin": "",
            "prixmax": "",
            "numero": "",
            "coordonnees": "",
            "archivage_statut": "0",
            "tri": "prix-asc",
            "page": "1",
            "route": "listing-location"
        }
        yield FormRequest(self.post_url,
                        callback=self.parse,
                        formdata=formdata,
                        dont_filter=True,
                        meta={'property_type': "apartment"})

            
    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)["data"]
        items = data["insee"]
        for item in items:
            f_url = item["route"]
            lat = item["x"]
            lng = item["y"]
            city = item["ville"]
            zipcode = item["departement"]     
            external_id = item["idhabit"]
            rent = item["prix"]
            room = item["piece"]
            
            yield Request(f_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'), 'lat':lat, 'lng': lng, 'city':city, 'zipcode': zipcode, 'external_id': external_id, 'rent': rent, 'room':room})
         
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))
        
        external_id = response.meta.get("external_id")
        if external_id:
            item_loader.add_value("external_id", str(external_id))
        
        title = response.xpath("//h1[@class='titre']/span/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        rent = response.meta.get("rent")
        if rent:
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        room_count = response.meta.get("room")
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        city = response.meta.get("city")
        if city:
            item_loader.add_value("city", city)
            item_loader.add_value("address", city)
        
        zipcode = response.meta.get("zipcode")
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        latitude = response.meta.get("lat")
        if latitude:
            item_loader.add_value("latitude", str(latitude))
        
        longitude = response.meta.get("lng")
        if longitude:
            item_loader.add_value("longitude", str(longitude))

        square_meters = response.xpath("//li[@class='c_surface']/span[@class='bloc-champ']/span[1]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip())

        utilities = "".join(response.xpath("//div[@class='info_prix-hai']/text()[contains(.,'Charges')]").getall())
        if utilities:
            utilities = utilities.split("Charges : ")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)

        deposit = "".join(response.xpath("//div[@class='info_prix-hai']/text()[contains(.,'garantie')]").getall())
        if deposit:
            deposit = deposit.split("garantie : ")[1].split("€")[0].strip()
            item_loader.add_value("deposit", deposit)

        floor = response.xpath("//li[@class='c_etage']/span[@class='bloc-champ']/span[1]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        details = " ".join(response.xpath("//li/@data-search").getall())
        if details:
            if details and "parking" in details.lower():
                item_loader.add_value("parking", True)
        
        balcony = "".join(response.xpath("//li[@class='c_nbbalcon']/span[@class='bloc-champ']/span[1]/text()").getall())
        if balcony:
            balcony = balcony.strip()
            if int(balcony) > 0:
                item_loader.add_value("balcony", True)

        desc = " ".join(response.xpath("//div[@class='col-sm-8']/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        images = [x for x in response.xpath("//div[@id='carouselImages']//div/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images) 
        
        item_loader.add_value("landlord_name", "Avenir Immobilier")
        item_loader.add_value("landlord_phone", "04 67 42 04 04")

        yield item_loader.load_item()