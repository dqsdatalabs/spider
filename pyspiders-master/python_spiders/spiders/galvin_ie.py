# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'galvin_ie'
    execution_type='testing' 
    country='ireland'
    locale='en'
    external_source = "Galvin_PySpider_ireland"

    def start_requests(self):
        url = "https://agent.daft.ie/search.daft?key=rqyzqu7fz9ykqd78874&search_type=rental&s[agreed]=0&css=https://galvin.ie/daftv1.css&domain=galvin.ie"
        yield Request(url, callback=self.parse)   

    # 1. FOLLOWING
    def parse(self, response):
       
        for item in response.xpath("//div[@class='listing']//h1/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
        next_button = response.xpath("//span[@id='listings_next']/a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse,)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        property_type=response.xpath("//div[@class='listing_ber']/following-sibling::h2/following-sibling::text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))

        title=response.xpath("//div[@id='property']/h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//div[@id='property']/h1/text()").get()
        if adres:
            item_loader.add_value("address",adres)

        rent=response.xpath("//div[@class='listing_ber']/following-sibling::h2/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[1].split("Mont")[0].replace(",","").strip())
        item_loader.add_value("currency","GBP")

        desc="".join(response.xpath("//div[@id='property_description']/text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        images=[x.split("   ")[0] for x in response.xpath("//img[@class='photo_thumbnail']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        room_count= response.xpath("//div[@class='listing_ber']/following-sibling::h2/following-sibling::br/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("bed")[0])
        bathroom_count= response.xpath("//div[@class='listing_ber']/following-sibling::h2/following-sibling::br/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split("bath")[0].split(",")[-1].strip())
        features=response.xpath("//h2[.='Facilities']/following-sibling::ul//li//text()").getall()
        if features:
            for i in features:
                if "Parking" in i:
                    item_loader.add_value("parking",True) 
                if "Dishwasher" in i:
                    item_loader.add_value("dishwasher",True) 
                if "Washing Machine" in i:
                    item_loader.add_value("washing_machine",True) 
        name=response.xpath("//b[.='Contact Name:']/following-sibling::text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//b[.='Phone:']/following-sibling::text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        email=response.xpath("//div[@class='col-xs-12 col-sm-12 col-md-12 col-lg-12 tabelka']//a[contains(@href,'mailto')]/text()").get()
        if email:
            item_loader.add_value("landlord_email",email)
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "huis" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None