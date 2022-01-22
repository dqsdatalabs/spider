# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'letsgoco_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        yield Request("http://www.letsgoco.co.uk/properties-for-let/", 
                    callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'PropertyContainer')]"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'View Property')]/@href").get())
            property_type = " ".join(item.xpath(".//div[contains(@class,'PropertyDecrip')]/p//text()").getall())
            let_agreed = item.xpath(".//span[@class='PropertyStatus']/text()[contains(.,'LET AGREED')]").get()
            if not let_agreed and get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Letsgoco_Co_PySpider_united_kingdom")
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("title", address)
            item_loader.add_value("address", address)
            
            zipcode = address.split(" ")[-1]
            item_loader.add_value("zipcode", zipcode)
            
            city = address.split(zipcode)[0].strip().strip(",").split(",")[-1]
            item_loader.add_value("city", city.strip())

        rent = response.xpath("//span[@class='Pricing']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace("£",""))
        item_loader.add_value("currency", "GBP")
        
        description = " ".join(response.xpath("//h2/following-sibling::p[1]//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [x for x in response.xpath("//div[@class='GallerySlider']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        deposit = response.xpath("//p/strong[contains(.,'Deposit')]/following-sibling::text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace("£",""))
        
        furnished = response.xpath("//p/strong[contains(.,'Furnish')]/following-sibling::text()").get()
        if furnished and "un" not in furnished.lower():
            item_loader.add_value("furnished", True)
        
        item_loader.add_value("landlord_name", "Lets Go Co")
        item_loader.add_value("landlord_phone", "0121 296 9596")
        item_loader.add_value("landlord_email", "matt@letsgoco.co.uk")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None