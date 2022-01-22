# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = "bonjour_oscar_com" # LEVEL 1
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = 'Bonjour_Oscar_PySpider_france' 
    
    def start_requests(self):
        start_urls = [
            {
                "url": "https://bonjour-oscar.com/recherche-appartement?property_type=2&postal_code=&price_max=&subtype=&reference=&category_id=&room=&bedroom=&area_min=&area_max=", 
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='details']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//h2[@class='title']/text()").get()
        if title:
            item_loader.add_value("title", title)
            if title and "studio" in title.lower():
                item_loader.add_value("property_type", "studio")
            elif title and "parking" in title.lower():
                return
            elif title and "box" in title.lower():
                return
            else:
                item_loader.add_value("property_type", "apartment")
        external_id=response.xpath("//p[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())

        rent=response.xpath("//div[@class='price']/text()").get()
        if rent:
            rent=rent.split('€')[0].replace(" ","")
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        square_meters=response.xpath("(//i[@class='fa icon-m2']/parent::div/text())[2]").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].strip())
        room_count=response.xpath("//span[@class='label'][contains(.,'pièces')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        floor=response.xpath("(//i[@class='fa icon-ler-etage icon-free-size']/parent::div/text())[2]").get()
        if floor:
            floor = re.findall(r'\d+', floor)
            if floor:
                item_loader.add_value("floor",floor)

        city=response.xpath("//span[@class='label'][contains(.,'Ville')]/following-sibling::span/text()").get()
        if city:
            item_loader.add_value("city",city)
            address = city
        
        zipcode=response.xpath("//span[@class='label'][contains(.,'postal ')]/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
            address = address + " " + zipcode
            item_loader.add_value("address", address)
        
        deposit=response.xpath("//span[@class='label'][contains(.,'Dépôt ')]/following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split('€')[0].replace(" ","").strip())

        utilities=response.xpath("//span[@class='label'][contains(.,'Honoraires ')]/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split('€')[0].replace(" ","").strip())

        desc=response.xpath("//div[@class='content text-left']/p/text()").getall()
        if desc:
            item_loader.add_value("description",desc)

        latlng = response.xpath("//script[contains(.,'BravoMapEngine')]/text()").get()
        if latlng:
            latitude = latlng.split('center: [')[-1].split(',')[0]
            item_loader.add_value("latitude", latitude)
            longitude = latlng.split('center: [')[-1].split(',')[1].split(']')[0].strip()
            item_loader.add_value("longitude", longitude)
        
        furnished = response.xpath("//span[@class='tick-red']/text()[contains(.,'Meublée')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//span[@class='tick-red']/text()[contains(.,'Ascenseur')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        images = [x for x in response.xpath("//div[@class='d-none d-lg-block col-lg-3']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        landlord_name = response.xpath("//div[@class='agent-name']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        landlord_phone = response.xpath("//span[@class='text'][contains(.,'Téléphone')]/parent::div/parent::div/div[@class='text-center']/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        
        yield item_loader.load_item()