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
    name = 'propertywiseltd_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://propertywise.ezadspro.co.uk/search?rent=1&radius=0&include_sold=0&format=photo&sort=newest&page_size=6&page=1']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[contains(@class,'row photo-row')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        next_page = response.xpath("//li[contains(@class,'pagination-next')]//@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        item_loader.add_value("external_source", "Propertywiseltd_PySpider_united_kingdom")
        title = response.xpath("//h3/text()").get()
        if get_p_type_string(title):
            item_loader.add_value("property_type", get_p_type_string(title))
        else: return
        
        item_loader.add_value("external_link", response.url.split("/")[-1])
        
        
        if title:
            item_loader.add_value("title", title)
            
            if "bed" in title.lower():
                room_count = title.lower().split("bed")[0].strip()
                item_loader.add_value("room_count", room_count)
        
        address = response.xpath("//address//text()").get()
        if address:
            address = address.strip(",")
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-1].strip())

        rent = response.xpath("//h3[contains(.,'£')]/text()").get()
        if rent:
            rent = rent.split("£")[1].strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        description = " ".join(response.xpath("//div[@id='property-body']//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        images = [x.split("(")[1].split(")")[0] for x in response.xpath("//div[@class='carousel']//@style").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//div/@data-marker-json").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat":')[1].split(',')[0]
            longitude = latitude_longitude.split('lng":')[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        washing_machine = response.xpath("//li[contains(.,'Washing Machine')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        parking = response.xpath("//li[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        item_loader.add_value("landlord_name", "Property Wise")
        item_loader.add_value("landlord_phone", "01454855020")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("studio" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "t2" in p_type_string.lower() or "t3" in p_type_string.lower() or "t4" in p_type_string.lower() or "t5" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower() or "house" in p_type_string.lower()):
        return "house"
    else:
        return None