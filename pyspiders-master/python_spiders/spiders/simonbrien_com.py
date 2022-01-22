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
    name = 'simonbrien_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Simonbrien_PySpider_united_kingdom"
    start_url = "https://www.simonbrien.com/property-for-rent"
    def start_requests(self): # LEVEL 1
        yield Request(url=self.start_url,callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//a[@class='prop-card']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
            seen = True
        if page == 2 or seen:
            nextpage=response.xpath("//li[@class='next']/a/@href").get()
            if nextpage:
                yield Request(response.urljoin(nextpage), callback=self.parse, meta={"page": page+1,})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source",self.external_source)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//h1[@class='prop-det-address-one']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        property_type=response.xpath("//span[.='Style']/following-sibling::span/text()").get()
        if property_type:
            if "Detached"==property_type or "Semi-Detached"==property_type or "Terrace"==property_type or "Townhouse"==property_type or "Detached Bungalow"==property_type:
                item_loader.add_value("property_type","house")
            else:
                item_loader.add_value("property_type",property_type.lower())
        rent=response.xpath("//span[.='Monthly']/following-sibling::span/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("£")[-1].split("pm")[0].replace(",",""))
        item_loader.add_value("currency","GBP")
        room=response.xpath("//span[.='Bedrooms']/following-sibling::span/text()").get()
        if room:
            item_loader.add_value("room_count",room)
        description=response.xpath("//div[@class='prop-det-text']/text()").get()
        if description:
            item_loader.add_value("description",description)
        images=[response.urljoin(x.split("background-image:url('")[-1].split("');")[0]) for x in response.xpath("//div[@class='slide gallery-btn']/@style").getall()]
        if images:
            item_loader.add_value("images",images)
        latitude=response.xpath("//script[contains(.,'google.maps.Map(')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("google.maps.Map(")[-1].split("lat:")[1].split(",")[0].strip())
        longitude=response.xpath("//script[contains(.,'google.maps.Map(')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("google.maps.Map(")[-1].split("lng:")[1].split("}")[0].strip())
        features=response.xpath("//h2[.='Key Features']/following-sibling::div//div//text()").getall()
        if features:
            for i in features:
                if "parking" in i:
                    item_loader.add_value("parking",True)
                if "washing machine" in i:
                    item_loader.add_value("washing_machine",True)

 

            

        item_loader.add_value("landlord_name", "Simon Brien Residential")
        item_loader.add_value("landlord_phone", "02890 668888")
        item_loader.add_value("landlord_email", "southbelfast@simonbrien.com")

     
        yield item_loader.load_item()