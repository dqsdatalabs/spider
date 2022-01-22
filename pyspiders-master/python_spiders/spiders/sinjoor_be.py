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
import dateparser
from datetime import datetime
import math

class MySpider(Spider):
    name = 'sinjoor_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source='Sinjoor_PySpider_belgium'
    custom_settings = {
        "PROXY_ON":"True"
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://sinjoor.be/te-huur?type=appartement&priceMin=&priceMax=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://sinjoor.be/te-huur?type=villa-woning-hoeve&priceMin=&priceMax=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='estate-info']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})     
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        studio_check = response.xpath("//div[@class='title']/h1/text()").get()
        property_type = response.meta.get('property_type')
        if studio_check and "studio" in studio_check.lower():
            property_type = "studio"
        
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//div[@class='title']/h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        address = response.xpath("//div[@class='right']/div[@class='address']/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(',')[-1].strip().split(' ')[0]
            city = address.split(zipcode)[1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
                   
        square_meters = response.xpath("//section[@class='estate-info']//img[@alt='oppervlakte']/following-sibling::div/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", math.ceil(int(float(square_meters))))
        else:
            square_meters = response.xpath("//div[@class='item']/ul/li/span[contains(.,'Bewoonbare oppervlakte')]/following-sibling::span/text()").get()
            if square_meters:
                square_meters = square_meters.split('m')[0]
                item_loader.add_value("square_meters", math.ceil(int(float(square_meters))))
        
        rent = response.xpath("//div[@class='overlay']//div[@class='price']/text()").get()
        if rent:
            rent = rent.split('€')[0].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        room_count = response.xpath("//section[@class='estate-info']//img[@alt='slaapkamers']/following-sibling::div/text()").get()
        if property_type == "studio":
            item_loader.add_value("room_count", "1")
        if room_count:
            item_loader.add_value("room_count", room_count.strip())     
        bathroom_count = response.xpath("//section[@class='estate-info']//img[@alt='badkamers']/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        desc = "".join(response.xpath("//div[@class='container-sm']//div[@data-aos='fade-up']/p/span/span/span/text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc.strip())
        else:
            desc = "".join(response.xpath("//div[@class='container-sm']//div[@data-aos='fade-up']/p/span/text()").extract())
            if desc:
                desc = re.sub('\s{2,}', ' ', desc.strip())
                item_loader.add_value("description", desc.strip())
            else:
                desc = "".join(response.xpath("//div[@class='container-sm']//div[@data-aos='fade-up']/p/span/span/text()").extract())
                desc = re.sub('\s{2,}', ' ', desc.strip())
                item_loader.add_value("description", desc.strip())
        
        images = [x for x in response.xpath("//div[@class='bg-img']//a[@data-fancybox='gallery']/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        external_id = response.xpath("//div[@class='item']/ul/li/span[contains(.,'Referentie')]/following-sibling::span/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        floor = response.xpath("//div[@class='item']/ul/li/span[contains(.,'Verdiepingen')]/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor", floor)

        energy_label = response.xpath("(//div[@class='item']/ul/li/span[contains(.,'EPC peil')]/following-sibling::span/text())[1]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
                     
        pets_allowed = response.xpath("(//div[@class='item']/ul/li/span[contains(.,'uisdier')]/following-sibling::span/text())[1]").get()
        if pets_allowed:
            if "ja" in pets_allowed.lower():
                item_loader.add_value("pets_allowed", True)
        
        utilities = response.xpath("(//div[@class='item']/ul/li/span[contains(.,'lasten')]/following-sibling::span/text())[1]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[1].strip())
        
        terrace = response.xpath("(//div[@class='item']/ul/li/span[contains(.,'Terras')]/following-sibling::span/text())[1]").get()
        if terrace:
            if "ja" in terrace.lower():
                item_loader.add_value("terrace", True)
        elevator = response.xpath("(//div[@class='item']/ul/li/span[contains(.,'Lift')]/following-sibling::span/text())[1]").get()
        if elevator:
            if "ja" in elevator.lower():
                item_loader.add_value("elevator", True)
        parking = response.xpath("(//div[@class='item']/ul/li/span[contains(.,'garage')]/following-sibling::span/text())[1]").get()
        if parking:
            if "ja" in parking.lower():
                item_loader.add_value("parking", True)
        else:
            parking = response.xpath("(//div[@class='item']/ul/li/span[contains(.,'Parking')]/following-sibling::span/text())[1]").get()
            if parking:
                if "ja" in parking.lower():
                    item_loader.add_value("parking", True)
        
        latitude = response.xpath("//div[@id='estate-map-data']/div[@class='estate-latitude']/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        longitude = response.xpath("//div[@id='estate-map-data']/div[@class='estate-longitude']/text()").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
        landlord_name = response.xpath("normalize-space(//div[@class='container-xs']/h4/text())").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip().replace("\n",""))
        else:       
            item_loader.add_value("landlord_name", "SINJOOR MAKELAARS")
        
        phone = response.xpath("//div[@class='container-xs']/a/@href[contains(.,'tel')]/parent::a/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        
        email = response.xpath("//div[@class='container-xs']/a/@href[contains(.,'mail')]/parent::a/text()").get()
        if email:
            item_loader.add_value("landlord_email", email)
        
        yield item_loader.load_item()