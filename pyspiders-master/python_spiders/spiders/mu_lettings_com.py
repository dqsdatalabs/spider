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
    name = 'mu_lettings_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):
        yield Request("https://mu-property.com/property/?ct_ct_status%5B%5D=to-let&ct_city=&ct_beds_plus=&ct_baths_plus=&ct_price_from=&ct_price_to=&search-listings=true", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//li[contains(@class,'listing')]"):
            follow_url = response.urljoin(item.xpath(".//h5/a/@href").get())
            property_type = item.xpath(".//li[contains(@class,'property-type')]/span[@class='right']/text()").get()
            if get_p_type_string(property_type): 
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Mu_Lettings_PySpider_united_kingdom")
        address = ", ".join(response.xpath("//h1[@id='listing-title']/..//text()[.!='To Let ']").getall())
        if address:
            item_loader.add_value("address", re.sub('\s{2,}', ' ', address.strip()))
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("bathroom_count", "//ul[@class='propinfo marB0']/li[@class='row baths']/span[2]/text()")
        item_loader.add_xpath("room_count", "//ul[@class='propinfo marB0']/li[@class='row beds']/span[2]/text()")
    
        rent_string = " ".join(response.xpath("//h4[contains(@class,'price')]//text()").getall())
        if rent_string:
            item_loader.add_value("rent_string", rent_string)
        description = " ".join(response.xpath("//div[@id='listing-content']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        zipcode = response.xpath("//header[@class='listing-location']/p[@class='location marB0']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(",")[-1].strip())
            item_loader.add_value("city", zipcode.split(",")[0].strip())
        script_map = response.xpath("//script/text()[contains(.,'}setMapAddress(')]").get()
        if script_map:
            latlng = script_map.split('}setMapAddress("')[1].split('"')[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())

        floor = response.xpath("//li[contains(.,'Floor')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("Floor")[0].strip())
        energy_label = response.xpath("//li[contains(.,'EPC Rating:')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(":")[-1].split("(")[0].strip())
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'Garage')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
    
        floor_plan_images = [x for x in response.xpath("//li[contains(.,'FLOORPLAN')]/a/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        images = [x for x in response.xpath("//div[@id='carousel']//li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "Michael Usher Sales & Lettings")
        item_loader.add_value("landlord_phone", "01276 534000")
        item_loader.add_value("landlord_email", "info@mu-property.com")
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None