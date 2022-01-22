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
    name = 'quinnestateagents_com'
    execution_type='testing' 
    country='ireland'
    locale='en'
    external_source = "Quinnestateagents_PySpider_ireland"

    def start_requests(self):
        url = "https://www.quinnestateagents.com/property-for-rent"
        yield Request(url, callback=self.parse)   

    # 1. FOLLOWING
    def parse(self, response):

        for url in response.xpath("//div[@class='property property-status-toLet']/@data-url").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)


    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)


        property_type=response.xpath("//td[.='Style']/following-sibling::td/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=" ".join(response.xpath("//span[@class='address-other']//span//text()").getall())
        if adres:
            item_loader.add_value("address",adres.replace("\n",""))
        zipcode=response.xpath("//span[@class='postcode']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.replace(",","").strip())
        rent=response.xpath("//span[@class='price-value ']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("£")[1].replace(",",""))
        item_loader.add_value("currency","EUR")
        images=[x for x in response.xpath("//a[@rel='photograph']/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        room_count= response.xpath("//td[.='Bedrooms']/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("bed")[0])
        bathroom_count=response.xpath("//td[.='Bathrooms']/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split("bath")[0].split(",")[-1].strip())
        description=response.xpath("//h2[.='Additional Information']/following-sibling::p/text()").get()
        if description:
            item_loader.add_value("description",description)
        features=response.xpath("//h2[.='Features']/following-sibling::ul//li//text()").getall()
        if features:
            for i in features:
                if "Parking" in i:
                    item_loader.add_value("parking",True) 

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