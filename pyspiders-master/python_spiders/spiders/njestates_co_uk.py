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
    name = 'njestates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Njestates_Co_PySpider_united_kingdom'
    thousand_separator = ','
    scale_separator = '.'
    start_urls = ['https://www.njestates.co.uk/properties/?department=residential-lettings']  # LEVEL 1

    custom_settings = {
        #"PROXY_ON":True,
        "PROXY_TR_ON": True,
        "HTTPCACHE_ENABLED": False,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 3,
    }
    handle_httpstatus_list = [401,403]
    # 1. FOLLOWING
    def parse(self, response):
        headers = {
            "authority": "www.njestates.co.uk",
            "method": "GET",
            "path": "/properties/?department=residential-lettings",
            "scheme": "https",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en,tr-TR;q=0.9,tr;q=0.8,en-US;q=0.7",
            "cache-control": "max-age=0",
            "referer": "https://www.njestates.co.uk/",
            "sec-ch-ua": '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
            "sec-ch-ua-mobile": "?0",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36"
         }

        for item in response.xpath("//div[@class='thumbnail']/a"):
            follow_url = item.xpath("./@href").get()
            status = item.xpath("//div[@class='flag']/text()").get()
            if status:
                yield Request(follow_url,headers=headers, callback=self.populate_item)
            
        next_page = response.xpath("//a[@class='next page-numbers']/@href").get()
        if next_page: 
            yield Request(next_page, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        rented = response.xpath("//div[@class='flag']//text()").extract_first()
        if rented:return

        desc = "".join(response.xpath("//div[contains(@class,'details')]//p//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: return
         
        item_loader.add_value("external_source", self.external_source)  
        item_loader.add_xpath("title", "//div[@class='details-left']/h2/text()")
        external_id = response.xpath("//span[contains(.,'Ref:')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[-1].strip())  
        address = response.xpath("//div[@class='details-left']/h2/text()").get()
        if address:
            address = address.split("BILLS")[0]
            item_loader.add_value("address", address.strip())    
            if "London" in address:
                zipcode = address.split("London")[-1].replace(",","").strip()
                item_loader.add_value("city", "London")
                item_loader.add_value("zipcode", zipcode)
            else:            
                zipcode = address.split(",")[-1].strip().split(" ")[-1]
                city =" ".join(address.split(",")[-1].strip().split(" ")[:-1])
                if city.replace(" ","").isalpha():
                    item_loader.add_value("city", city)
                    item_loader.add_value("zipcode", zipcode)
                else:
                    item_loader.add_value("city", address.split(",")[-2].strip())
                    item_loader.add_value("zipcode", address.split(",")[-1].strip())

        features = " ".join(response.xpath("//li[@class='feature-lines']/text()").getall())
        if features:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",features.replace(",",""))
            if unit_pattern:
                square=unit_pattern[0][0]
                sqm = str(int(float(square) * 0.09290304))
                item_loader.add_value("square_meters", sqm)
        
            if "unfurnished" in features.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in features.lower():
                item_loader.add_value("furnished", True)
        terrace = response.xpath("//li[@class='feature-lines']/text()[contains(.,'TERRACE')]").get()
        if terrace:
            item_loader.add_value("terrace", True)  
           
        item_loader.add_xpath("room_count","//div[@class='details-left']//div[img[@title='Bedrooms']]/span[@class='room-icons-numbers']/text()")
        item_loader.add_xpath("bathroom_count","//div[@class='details-left']//div[img[@title='Bathrooms']]/span[@class='room-icons-numbers']/text()")
     
        rent = response.xpath("//div[@class='details-left']/h4/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)
     
        description = " ".join(response.xpath("//div[@class='details-left']/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        images = [x for x in response.xpath("//div[@id='slider']//li//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)
  
        item_loader.add_value("landlord_name", "Nicholas James Estate Agents")
        item_loader.add_value("landlord_phone", "020 8886 9462")
        item_loader.add_value("landlord_email", "info@njestates.co.uk")
        lat_lng = response.xpath("//script[contains(.,' myLatlng = new google.maps.LatLng(')]/text()").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split(" myLatlng = new google.maps.LatLng(")[-1].split(",")[0])
            item_loader.add_value("longitude", lat_lng.split(" myLatlng = new google.maps.LatLng(")[-1].split(",")[1].split(")")[0].strip())
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None