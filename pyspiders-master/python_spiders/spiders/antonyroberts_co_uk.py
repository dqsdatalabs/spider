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
    name = 'antonyroberts_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.'
    external_source = "Antonyroberts_Co_PySpider_united_kingdom"

    def start_requests(self):
        url = "https://www.antonyroberts.co.uk/properties-for-rent/"
        yield Request(url, callback=self.parse)   

    # 1. FOLLOWING
    def parse(self, response):

        for url in response.xpath("//div/a[div[@class='prop-img_cont']][not(contains(.,'Let') or contains(.,'Under Offer'))]/@href").getall():
            yield Request(url, callback=self.populate_item)
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)
        f_text = " ".join(response.xpath("//div[@id='information']/div//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            print("-->",response.url)
            return

        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("?p=")[-1])
        
        address = response.xpath("//div[@class='propery-side-container']//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(',')[-1].strip())
            item_loader.add_value("city", address.split(',')[-2].strip())
            
        title = response.xpath("//div[@class='propery-side-container']//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        description = " ".join(response.xpath("//div[@id='information']/div//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())

        bathroom_count = response.xpath("//li//text()[contains(.,'Bath')][not(contains(.,'0 Bath'))]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Bath")[0].split(",")[-1].strip())

        room_count = response.xpath("//li//text()[contains(.,'Bedroom')][not(contains(.,'0 Bedroom'))]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Bedroom")[0].split(",")[-1].strip())
        elif "studio" in get_p_type_string(f_text):
            item_loader.add_value("room_count", "1")
            
        rent = response.xpath("//p[@class='property-price']//text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)
        
        images = [x for x in response.xpath("//div[@class='slick-property-gallery-holder']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath(
            "//script[contains(.,'lng')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(
                "lat:")[1].split(",")[0]
            longitude = latitude_longitude.split(
                "lng:")[1].split("}")[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Antony Roberts")
        item_loader.add_value("landlord_phone", "020 8940 9403")
        item_loader.add_value("landlord_email", "hello@antonyroberts.co.uk")

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
      