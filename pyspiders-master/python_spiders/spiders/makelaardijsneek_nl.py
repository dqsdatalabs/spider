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
    name = 'makelaardijsneek_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source="Makelaardijsneek_PySpider_netherlands_nl"
     # LEVEL 1
    
    # custom_settings = {
    #   "PROXY_ON": True,
    #   "RETRY_HTTP_CODES": [500, 503, 504, 400, 401, 403, 405, 407, 408, 416, 456, 502, 429, 307],
    #   "HTTPCACHE_ENABLED": False
      
    # }
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.makelaardijsneek.nl/aanbod"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='item']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        external_id=response.url
        if external_id:
            item_loader.add_value("external_id",external_id.split("/")[-1].split("-")[0])
        title=response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        price = response.xpath("//div[@class='inner-content']/span/span[@class='label' and contains(.,'Huur')]/following-sibling::span/text()").get()
        if price:
            item_loader.add_value("rent", price.split("€")[1].replace(".","").strip())
            item_loader.add_value("currency", "EUR")

            square = response.xpath("//li[span[contains(.,'Woonoppervlakte')]]/span[2]/text()").get()
            if square:
                item_loader.add_value("square_meters", square.split("m")[0].strip())

            images = [response.urljoin(x)for x in response.xpath("//span[@class='image-holder']//@src").extract()]
            if images:
                    item_loader.add_value("images", images)
                    

            item_loader.add_xpath("room_count","//li[span[contains(.,'slaapkamer')]]/span[2]/text()")

            property_type = response.xpath("//li[span[contains(.,'Soort object')]]/span[2]/text()").get()
            if property_type:
                if "Appartement" in property_type:
                    property_type = "apartment"
                elif "woonhuis" in property_type.lower():
                    item_loader.add_value("property_type", "apartment") 

            desc = "".join(response.xpath("//div[@class='text'][1]/text() | //div[@class='text2'][1]/div/text()").extract())
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
            location=response.xpath("//div[contains(@data-map,'coordinates')]/@data-map").get()
            if location:
                latitude=location
                item_loader.add_value("latitude",latitude.split("coordinates")[-1].split("lat':")[-1].split(",")[0].replace("{","").replace("'","").split(":")[-1])
                longitude=location
                item_loader.add_value("longitude",longitude.split("coordinates")[-1].split("lon':")[-1].split("}")[0].replace("{","").replace("'","").split(":")[-1])

            address = "".join(response.xpath("//h1//text()").extract()).strip()
            address = re.sub('\s{2,}', ' ', address)
            item_loader.add_value("address",address)
            city_zip = response.xpath("//h1/span/text()").get()
            if city_zip:
                zipcode = city_zip.split(" ")[0]
                city = city_zip.split(zipcode)[1].strip()
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)

            floor = response.xpath("//li[span[contains(.,'woonlagen')]]/span[2]/text()").get()
            if floor:
                item_loader.add_value("floor", floor.split(" ")[0])
            deposit=response.xpath("//div[contains(.,'Waarborgsom')]/text()").getall()
            if deposit:
                for i in deposit:
                    if "Waarborgsom" in i:
                        item_loader.add_value("deposit",i.split("€")[-1].strip().split(",")[0].replace(".",""))

            item_loader.add_xpath("energy_label", "//li[span[contains(.,'Energieklasse')]]/span[2]/text()")
            
            parking = response.xpath("//li[span[contains(.,'Parkeerfaciliteiten')]]/span[2]/text()[contains(.,'parkeren')]").get()
            if parking:
                item_loader.add_value("parking", True)
            
            item_loader.add_value("landlord_phone", "0515-431543")
            item_loader.add_value("landlord_email", "nfo@makelaardijsneek.nl")
            item_loader.add_value("landlord_name", "Makelaardij")
            
            yield item_loader.load_item() 