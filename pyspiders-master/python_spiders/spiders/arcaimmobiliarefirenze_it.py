# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'arcaimmobiliarefirenze_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Arcaimmobiliarefirenze_PySpider_italy"
    start_urls = ['http://www.arcaimmobiliarefirenze.it/immobili/affitto']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//h3/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"http://www.arcaimmobiliarefirenze.it/immobili/affitto?page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//li[contains(.,'Tipologia')]/span/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            return
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.url.split("/")[-1].split("-")[0])

        title = response.xpath("//title/text()").get()
        item_loader.add_value("title", title)
        
        rent = response.xpath("//span[@class='price']/text()").get()
        if rent:
            rent = rent.split(",")[0].split("€")[1].strip().replace(".","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        room_count = response.xpath("//li[contains(.,'Vani')]/span/text()").get()
        item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//li[contains(.,'Bagni')]/span/text()").get()
        item_loader.add_value("bathroom_count", bathroom_count)
        
        square_meters = response.xpath("//li[contains(.,'Metri')]/span/text()").get()
        item_loader.add_value("square_meters", square_meters)
        
        city = response.xpath("//li[contains(.,'Comune')]/span/text()").get()
        item_loader.add_value("city", city)
        
        address = response.xpath("//div[@class='infotext-detail']/h3/text()").get()
        if address:
            item_loader.add_value("address", f"{address} {city}")
        
        description = "".join(response.xpath("//div[@class='excerpt']/p/text()").getall())
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()  ))
        
        images = [x for x in response.xpath("//ul[@class='slides']//@src").getall()]
        item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat":')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('lng":')[1].split(',')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_value("landlord_name", "ARCA IMMOBILIARE")
        item_loader.add_value("landlord_phone", "+39 055.289210")
        item_loader.add_value("landlord_email", "info@arcaimmobiliarefirenze.it")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("trilocale" in p_type_string.lower() or "house" in p_type_string.lower() or "villetta" in p_type_string.lower() or "villino" in p_type_string.lower() or "villa" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    elif p_type_string and "monolocale" in p_type_string.lower():
        return "studio"
    else:
        return None