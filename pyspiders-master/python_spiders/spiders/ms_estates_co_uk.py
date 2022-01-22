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
    name = 'ms_estates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'    
    thousand_separator = ','
    scale_separator = '.'  
    start_urls = ['https://www.ms-estates.co.uk/Search?listingType=6&statusids=1&obc=Added&obd=Descending&areainformation=&radius=&bedrooms=&maxbedrooms=&minprice=&maxprice=']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//a[contains(.,'Full Detail')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.ms-estates.co.uk/Search?listingType=6&statusids=1&obc=Added&obd=Descending&areainformation=&radius=&bedrooms=&maxbedrooms=&minprice=&maxprice=&page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        desc = "".join(response.xpath("//h2[contains(.,'Summary')]/following-sibling::text()[1]").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: return
        item_loader.add_value("external_source", "Ms_estates_Co_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//h3/text()")
        address = response.xpath("//h3/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
       
        item_loader.add_xpath("rent_string", "//div[@class='fdPrice']/div/text()[normalize-space()]")

        if desc:
            item_loader.add_value("description", desc.strip())
 
        item_loader.add_xpath("room_count", "substring-before(//div[@class='fdRooms']/span/text()[contains(.,'bedroom')],'bedroom')")
        item_loader.add_xpath("bathroom_count", "substring-before(//div[@class='fdRooms']/span/text()[contains(.,'bathroom')],'bathroom')")
      
        balcony = response.xpath("//li[contains(.,'balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        furnished = response.xpath("//li[contains(.,'furnished') or contains(.,'Furnished')]//text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        images = [x for x in response.xpath("//div[@id='property-photos-device1']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)        
        floor_plan_images = [x for x in response.xpath("//div/a[contains(.,'Floor Plan')]/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)        
     
        item_loader.add_value("landlord_name", "MS Estates")
        item_loader.add_value("landlord_phone", "0115 912 0061")
        item_loader.add_value("landlord_email", "mail@ms-estates.co.uk")
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("villa" in p_type_string.lower() or "bedroom" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None