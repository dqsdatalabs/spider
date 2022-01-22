# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re 

class MySpider(Spider):
    name = 'besserco_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.besserco.com.au/wp-json/besser/v1/property/search?suburb=&property_type=Apartment&bedrooms=&bathrooms=&garage=&price_min=50&price_max=5000&list=lease&page=1&limit=1000",
                    "https://www.besserco.com.au/wp-json/besser/v1/property/search?suburb=&property_type=Unit&bedrooms=&bathrooms=&garage=&price_min=50&price_max=5000&list=lease&page=1&limit=1000"
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.besserco.com.au/wp-json/besser/v1/property/search?suburb=&property_type=House&bedrooms=&bathrooms=&garage=&price_min=50&price_max=5000&list=lease&page=1&limit=1000",
                    "https://www.besserco.com.au/wp-json/besser/v1/property/search?suburb=&property_type=Townhouse&bedrooms=&bathrooms=&garage=&price_min=50&price_max=5000&list=lease&page=1&limit=1000",
                    "https://www.besserco.com.au/wp-json/besser/v1/property/search?suburb=&property_type=Villa&bedrooms=&bathrooms=&garage=&price_min=50&price_max=5000&list=lease&page=1&limit=1000",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.besserco.com.au/wp-json/besser/v1/property/search?suburb=&property_type=Studio&bedrooms=&bathrooms=&garage=&price_min=50&price_max=5000&list=lease&page=1&limit=1000"
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data["properties"]:
            item_loader = ListingLoader(response=response)
            
            
            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_value("external_source", "Besserco_Com_PySpider_australia")   
            item_loader.add_value("title", item["headline"])
            address = item["street_address"]
            city = item["suburb"]
            item_loader.add_value("address",address+", "+city)
            item_loader.add_value("zipcode", item["postcode"])
            item_loader.add_value("city", city)
            item_loader.add_value("bathroom_count",item["bathrooms"])
            item_loader.add_value("room_count",item["bedrooms"])
            item_loader.add_value("description", item["description"])
            rent = item["price"]
            if rent:
                rent = rent.split("$")[-1].lower().split("p")[0].strip().replace(',', '')
                item_loader.add_value("rent", int(float(rent)) * 4)
            item_loader.add_value("currency", 'AUD')
            parking = item["carspaces"]
            if parking:
                item_loader.add_value("parking", True) if parking != "0" else item_loader.add_value("parking", False)
            item_loader.add_value("latitude", item['latitude'])
            item_loader.add_value("longitude", item['longitude'])
            follow_url = f"https://www.besserco.com.au/{item['url']}"
            item_loader.add_value("external_link", follow_url)
            yield Request(follow_url, callback=self.populate_item, meta={"item_loader":item_loader})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = response.meta["item_loader"]
        externalid=response.xpath("//link[@rel='shortlink']/@href").get()
        if externalid:
            item_loader.add_value("external_id",externalid.split("p=")[-1])
   
        balcony = response.xpath("//div[@class='post-item__data--content']//text()[contains(.,'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True) 
        images = [x for x in response.xpath("//div[@class='modal--property-gallery--large']/div/img/@data-lazy-src").getall()]
        if images:
            item_loader.add_value("images", images)
      
        item_loader.add_value("landlord_name", "Besser + co")
        item_loader.add_value("landlord_phone", "0395311000")
        item_loader.add_value("landlord_email", "info@besserco.com.au")
        yield item_loader.load_item()