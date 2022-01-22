# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
from  geopy.geocoders import Nominatim
import re

class MySpider(Spider):
    name = 'cogir_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source= 'Cogir_PySpider_france_fr'
    custom_settings = { 
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.cogir.fr/fr/listing-location.html?loc=location&type%5B%5D=maison&surfacemin=&prixmax=&numero=&coordonnees=&archivage_statut=0&tri=&page=1",
                "property_type" : "house"
            },
            {
                "url" : "https://www.cogir.fr/fr/listing-location.html?loc=location&type%5B%5D=appartement&surfacemin=&prixmax=&numero=&coordonnees=&archivage_statut=0&tri=&page=1",
                "property_type" : "apartment"
            },
            
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//a[@class='item-link']/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
            seen = True
        

        if page == 2 or seen:
            url = response.url.split("&page")[0] + f"&page={page}"
            yield Request(
                url=url,
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")}
            ) 
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        title = "".join(response.xpath("//h1/span//text()").extract())
        item_loader.add_value("title", re.sub("\s{2,}", " ", title))
        
        rent="".join(response.xpath("//li[@class='c_prix']/span[2]//text()").getall())
        if rent:
            price=rent.strip().replace("\xa0","")
            item_loader.add_value("rent_string", price)
        
        square_meters="".join(response.xpath("//ul/li[contains(@class,'surface')]//span[contains(@class,'champ')]/text()").getall())
        if "," in square_meters:
            square_mt=square_meters.split(",")[0]
            item_loader.add_value("square_meters", square_mt.strip())
        elif square_meters:
            item_loader.add_value("square_meters", square_meters.strip())
        
        room_count="".join(response.xpath("//ul/li[contains(@class,'c_piece')]//span[contains(@class,'champ')]/text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        latitude_longitude = response.xpath("//script[contains(.,'centerLngLat')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("lat: '")[1].split("'")[0]
            longitude = latitude_longitude.split("lng: '")[1].split("'")[0]
            geolocator = Nominatim(user_agent=response.url)
            try:
                location = geolocator.reverse(latitude + ', ' + longitude, timeout=None)
                if location.address:
                    address = location.address
                    if location.raw['address']['postcode']:
                        zipcode = location.raw['address']['postcode']
            except:
                address = None
                zipcode = None
            if address:
                item_loader.add_value("address", address)
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
            
        external_id="".join(response.xpath("//ul/li[contains(@class,'c_numero')]//span[contains(@class,'champ')]/text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        desc="".join(response.xpath("//div/h2[contains(@class,'info_titre')]//parent::div/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        images=[x for x in response.xpath("//div[contains(@class,'carousel-item')]/a/@href").extract()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name","COGIR LOCATION")
        item_loader.add_value("landlord_phone","02 99 79 51 01")
        item_loader.add_value("landlord_email","contact.location@cogir.fr")
            
        utilties="".join(response.xpath("//div[@class='info_prix-hai'][contains(.,'Charges :')]/text()").getall())
        if utilties:
            item_loader.add_value("utilities", utilties.split('Charges :')[1].split('€')[0].strip())
        
        deposit="".join(response.xpath("//div[@class='info_prix-hai'][contains(.,'Dépôt de garantie')]/text()").getall())
        if deposit:
            item_loader.add_value("deposit", deposit.split('Dépôt de garantie :')[1].split('€')[0].strip())
        
        elevator=response.xpath("//ul/li[contains(@class,'c_ascenseur')]//span[contains(@class,'champ')]/text()[contains(.,'Oui')]").get()
        if elevator:
            item_loader.add_value("elevator",True)
        
        terrace=response.xpath("//ul/li[contains(@class,'c_nbterrasse')]//span[contains(@class,'champ')]/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
            
        parking=response.xpath("//ul/li[contains(@class,'c_garage')]//span[contains(@class,'champ')]/text()[contains(.,'Oui')]").get()
        garage=response.xpath("//ul/li[contains(@class,'c_parking')]//span[contains(@class,'champ')]/text()[contains(.,'Oui')]").get()
        if parking or garage:
            item_loader.add_value("parking",True)

        yield item_loader.load_item()

