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
    name = 'homelink_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    start_urls = ['https://www.homelink.co.uk/rent']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False

        for item in response.xpath("//article//a/@href").extract():
            follow_url = response.urljoin(item)           
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.homelink.co.uk/rent?pagenumber={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Homelink_PySpider_united_kingdom")
        external_id = response.xpath("//h3[contains(.,'Ref No')]/text()").re_first(r"\d+")
        if external_id:
            item_loader.add_value("external_id", external_id)
            
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        rent = response.xpath("//h2[contains(.,'£')]/text()").re_first(r'£(\d+.\d+)')
        if rent:
            item_loader.add_value("rent", rent.replace(",",""))
        item_loader.add_value("currency", "GBP")
        
        address = response.xpath("//h2/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            zipcode = address.split(",")[-1]
            if zipcode:
                item_loader.add_value("zipcode", zipcode.strip())
        
        item_loader.add_value("city", "LONDON")
        
        desc = "".join(response.xpath("//section[contains(@class,'propertyDetail')]//p/text()").getall())
        if desc:
            desc = re.sub(r"\s{2,}", "", desc)
            item_loader.add_value("description", desc)

            apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex"]
            house_types = ['mobile home','park home','character property',
                'chalet', 'bungalow', 'maison', 'house', 'home', 'villa',
                'holiday complex', 'cottage', 'semi-detached']
            studio_types = ["studio"]
            if any (i in desc.lower() for i in studio_types):
                item_loader.add_value('property_type','studio')
            elif any (i in desc.lower() for i in apartment_types):
                item_loader.add_value('property_type','apartment')
            elif any (i in desc.lower() for i in house_types):
                item_loader.add_value('property_type','house')
            else: return
        room_count = response.xpath("//img[@alt='Bedrooms']/../span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//img[@alt='Bathrooms']/../span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        

        features = "".join(response.xpath("//div[@class='featured']//text()").getall())
        
        if "dishwasher" in features.lower():
            item_loader.add_value("dishwasher", True)
        
        if "pet" in features.lower():
            item_loader.add_value("pets_allowed", True)
        
        if "furnished" in features.lower():
            item_loader.add_value("furnished", True)
        
        if "parking" in features.lower():
            item_loader.add_value("parking", True)

        images = [i for i in response.xpath("//div[@class='slide']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        lat = response.xpath("//script[contains(.,'lat')]").re_first(r"{ lat:(\d+.\d+) ,")
        lng = response.xpath("//script[contains(.,'lat')]").re_first(r"lng:(-*\d+.\d+) }")
        if lat and lng:
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        
        
        floor_plan_images = [x for x in response.xpath("//img[@alt='Floor Plan']/@src").getall()]
        item_loader.add_value("floor_plan_images", floor_plan_images)
        
        item_loader.add_value("landlord_name", " Homelink Lettings & Estates")
        item_loader.add_value("landlord_phone", "0208 882 2112")
        item_loader.add_value("landlord_email", "info@homelink.co.uk")
        
       
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
        