# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import datetime
from datetime import date
import dateparser
import re

class MySpider(Spider):
    name = 'cabinetdm_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source='Cabinetdm_PySpider_france'

    def start_requests(self):

        url = "https://www.cabinetdm.com/fr/locations"
        yield Request(url,callback=self.parse,)

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in  response.xpath("//div[@class='buttons']//a[@class='button']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        dontallow=item_loader.get_output_value("title")
        if dontallow and "commercial" in dontallow.lower():
            return 
        property_type =item_loader.get_output_value("title")
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        external_id=response.xpath("//li[contains(.,'Ref')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(".")[-1].strip())
        rent=response.xpath("//li[contains(.,'Mois')]/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip().replace(" ",""))
        square_meters=response.xpath("//li[contains(.,'m²')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(" ")[0])
        description=response.xpath("//p[@id='description']//text()").getall()
        if description:
            item_loader.add_value("description",description)
        room_count=response.xpath("//li[contains(.,'chambres')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0])
        bathroom_count=response.xpath("//li[contains(.,'salles')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip().split(" ")[0])
        images=[x for x in response.xpath("//div[@class='item resizePicture']/a/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        name=response.xpath("//p[@class='smallIcon userName']/strong/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//p[@class='smallIcon userName']/a/@href").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        email=response.xpath("//p[@class='smallIcon userName']/a/@href").get()
        if email:
            item_loader.add_value("landlord_email",email)
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "maison" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None