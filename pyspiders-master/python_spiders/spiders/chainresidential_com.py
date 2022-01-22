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
    name = 'chainresidential_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.chainresidential.com/lettings.php"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='view-detail']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.chainresidential.com/lettings.php?page={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        f_text = " ".join(response.xpath("//div[@class='prop_description']/p/text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return

        ext_id = response.url.split("id=")[1].strip()
        if ext_id:
            item_loader.add_value("external_id", ext_id)

        item_loader.add_value("external_source", "Chainresidential_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//div[@class='entry-wrap']/h3/text()")        
        address =", ".join(response.xpath("//div[@class='entry-wrap']/h3/text() | //div[@class='entry-wrap']/div[@class='property-address']/text()").extract())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
            city_zip = address.split(",")[-1].strip()
            if len(city_zip.split(" ")) == 1:
                if city_zip.isalpha():
                    city = city_zip
                    zipcode = ""
                else:
                    zipcode = city_zip
                    city = address.split(",")[-2].strip()
            else:
                zipcode = city_zip.split(" ")[-1]
                if zipcode.isalpha():
                    city = city_zip
                    zipcode = ""
                else:
                    city = city_zip.replace(zipcode,"")

            if city:
                if "terrace" not in city.lower() or "street" not in city.lower():
                    item_loader.add_value("city", city.strip())
            if zipcode:
                item_loader.add_value("zipcode", zipcode.strip())

        rent = response.xpath("//div[@class='property-price']/text()").extract_first()
        if rent:
            rent = rent.lower().split('£')[-1].strip().replace(',', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))     
        item_loader.add_value("currency", 'GBP')           
        parking = response.xpath("//ul[@class='property-list-info']/li/div[contains(.,'Parking')]/span/text()").extract_first()
        if parking:   
            item_loader.add_value("parking",True) 
        square = response.xpath("//ul[@class='property-list-info']/li/div[contains(.,'Area Size')]/span/text()").extract_first()
        if square:   
            square = square.lower().split("s")[0].strip()
            sqm = str(int(float(square) * 0.09290304))
            item_loader.add_value("square_meters", int(sqm))
        
        room_count = response.xpath("//div[@class='entry-wrap']//li[i[@class='fa fa-bed']]/span/text()[normalize-space()]").extract_first()
        if room_count:   
            item_loader.add_value("room_count",room_count)
        elif "studio" in get_p_type_string(f_text):
            item_loader.add_value("room_count","1")   
        else:
            room_count = response.xpath("//ul[@class='property-list-info']/li/div[contains(.,'Rooms') and not(contains(.,'Living'))]/span/text()").extract_first()
            if room_count:   
                item_loader.add_value("room_count",room_count)    
       
        bathroom_count = response.xpath("//ul[@class='property-list-info']/li/div[contains(.,'Bathroom')]/span/text()[normalize-space()]").extract_first()
        if not bathroom_count:
            bathroom_count = response.xpath("//div[@class='entry-wrap']//li[i[@class='fa fa-bath']]/span/text()[normalize-space()]").extract_first()
        if bathroom_count:   
            item_loader.add_value("bathroom_count",bathroom_count)    
        images = [response.urljoin(x) for x in response.xpath("//div[@class='property-slider']/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
       
        desc = " ".join(response.xpath("//div[@class='prop_description']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        item_loader.add_value("external_id", response.url.split("id=")[1])
        
        item_loader.add_value("landlord_name", "Chain Residential")
        item_loader.add_value("landlord_phone", "0207 7046 999")
        item_loader.add_value("landlord_email", "info@chainresidential.com")   
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None