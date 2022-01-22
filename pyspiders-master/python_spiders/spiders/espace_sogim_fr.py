# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json, math
import lxml, js2xml

class MySpider(Spider):
    name = 'espace_sogim_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Espace_Sogim_PySpider_france'
    
    def start_requests(self):
        start_urls = [
            {
                "url" : ["https://espace-sogim.fr/fr/louer"]
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//li[@class='property initial']/div/figure/a//@href").extract():
            follow_url = response.urljoin(item)  
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        external_id = response.xpath("(//div[@class='summary details clearfix']/ul/li[contains(.,'Référence')]/span/text())[1]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        property_type = response.xpath("(//li[@class='module-breadcrumb-tab']/a)[2]/text()").get()
        if property_type:
            if property_type and "appartement" in property_type.lower():
                item_loader.add_value("property_type", "apartment")
            elif property_type and "maison" in property_type.lower():
                item_loader.add_value("property_type", "house")
            else: return
        title = response.xpath("//div[@data-module-id='86211']/h1/text()").get()
        if title:
            item_loader.add_value("title", title)
        rent = response.xpath("//div[@data-module-id='86211']/p[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split('€')[0].strip().replace(" ",""))
        item_loader.add_value("currency", "EUR")
        deposit = response.xpath("//ul/li[contains(.,'Dépôt')]/span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[0].strip().replace(" ","").replace(" ",""))
        utility = response.xpath("//ul/li[contains(.,'Provision sur charges')]/span/text()").get()
        if utility:
            utility = utility.split('€')[0].strip().replace(" ","").replace(",",".")
            utility = math.ceil(float(utility.strip()))
            item_loader.add_value("utilities", utility)
        city = response.xpath("//div[@data-module-id='86211']/h1/span/text()").get()
        if city:
            item_loader.add_value("city", city)
            item_loader.add_value("address",city)

        room_count = response.xpath("(//div[@class='summary details clearfix']/ul/li[contains(.,'Pièces ')]/span/text())[1]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(' ')[0].strip())

        square_meters = response.xpath("(//div[@class='summary details clearfix']/ul/li[contains(.,'Surface ')]/span/text())[1]").get()
        if square_meters:
            square_meters = square_meters.split(' ')[0].strip()
            square_meters = math.ceil(float(square_meters.strip()))
            item_loader.add_value("square_meters", square_meters)
        floor = response.xpath("(//div[@class='summary details clearfix']/ul/li[contains(.,'Étage ')]/span/text())[1]").get()
        if floor:
            floor = floor.split('étage')[0].strip().replace(""," ").split()
            floor = floor[0]
            if floor.isdigit():
                item_loader.add_value("floor", floor.strip())

        desc = "".join(response.xpath("//div[@data-module-id='86185']/p[@class='comment']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        elevator = response.xpath("//ul/li/text()[contains(.,'Ascenseur')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        parking = response.xpath("//ul/li/text()[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        pets_allowed = response.xpath("//ul/li/text()[contains(.,'Animaux')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)

        images = [x for x in response.xpath("//a[contains(@class,'click-fullscreen')]/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
          
        landlord_name = response.xpath("(//div[@class='info']/h3/a/text())[1]").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        landlord_phone = response.xpath("(//div[@class='info']/p/span[@class='phone']/a/text())[1]").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        landlord_email = response.xpath("(//div[@class='info']/p/span[@class='email']/a/text())[1]").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()