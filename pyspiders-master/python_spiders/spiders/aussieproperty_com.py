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
    name = 'aussieproperty_com'
    execution_type='testing'
    country='australia'
    locale='en'
    headers = {
        'authority': 'apiv2.completeempire.com',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'x-authentication': '1e23c5f7-637f-46c5-bb09-fd2066e72f07:v2QfNJPr2CRbo9//dMJelxRAOn428+rByHCtIs6GvlI=',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://www.aussieproperty.com',
        'referer': 'https://www.aussieproperty.com/',
        'accept-language': 'tr,en;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
    }
    custom_settings = {
        "PROXY_ON":"True"
    }

    def start_requests(self):
        start_url = "https://apiv2.completeempire.com/rentals/properties?sort=ListedDate-desc&page=1&pageSize=10&group=&filter=Type~eq~%272%27"
        yield Request(start_url, headers=self.headers, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen=False
        data = json.loads(response.body)
        print(response.url)
        for item in data["Data"]:
            title = item["name"].replace(" ", "-").replace(",", "")
            seen_url = "https://www.aussieproperty.com/property/details/" + title + "/" + item["id"]
            is_leased = True if item["property_status"] == "Leased" else False
            property_type = item["property_type"]
            if property_type:
                if get_p_type_string(property_type) and not is_leased:
                    follow_url = "https://apiv2.completeempire.com/properties/" + item["id"]
                    hdrs = {
                        'authority': 'apiv2.completeempire.com',
                        'accept': 'application/json, text/javascript, */*; q=0.01',
                        'x-authentication': '1e23c5f7-637f-46c5-bb09-fd2066e72f07:v2QfNJPr2CRbo9//dMJelxRAOn428+rByHCtIs6GvlI=',
                        'origin': 'https://www.aussieproperty.com',
                        'referer': 'https://www.aussieproperty.com/',
                        'accept-language': 'tr,en;q=0.9',
                        'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
                    }
                    yield Request(follow_url, headers=hdrs, callback=self.populate_item, meta={"property_type":get_p_type_string(property_type), "seen_url":seen_url, "item":item})
            seen=True
        if page ==2 or seen:        
            f_url = response.url.replace(f"page={page-1}", f"page={page}")
            hdr = {
                'authority': 'apiv2.completeempire.com',
                'accept': 'application/json, text/javascript, */*; q=0.01',
                'x-authentication': '1e23c5f7-637f-46c5-bb09-fd2066e72f07:v2QfNJPr2CRbo9//dMJelxRAOn428+rByHCtIs6GvlI=',
                'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'origin': 'https://www.aussieproperty.com',
                'referer': 'https://www.aussieproperty.com/',
                'accept-language': 'tr,en;q=0.9',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
            }
            yield Request(f_url, headers=hdr, callback=self.parse, meta={"page": page+1})
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.meta.get('seen_url'))
        details_data = json.loads(response.body) 
        item = response.meta.get('item') 
        item_loader.add_value("description", details_data["description"])        
        item_loader.add_value("external_source", "Aussieproperty_PySpider_australia")       
        item_loader.add_value("title", str(item["name"]))
        item_loader.add_value("latitude", str(item["location"]["latitude"]))
        item_loader.add_value("longitude", str(item["location"]["longitude"]))
        zipcode = item["address"]["postcode"]
        city = item["address"]["city"]
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("address",item["name"]+", "+zipcode )
        item_loader.add_value("room_count", item["bedrooms"])
        item_loader.add_value("bathroom_count", item["bathrooms"])
        
        rent = item["weekly_rental_amount"]
        if rent != 0:
            item_loader.add_value("rent", int(float(rent)) * 4)
        item_loader.add_value("currency", 'USD')
        deposit = item["bond"]
        if deposit != 0:
            item_loader.add_value("deposit",int(float(deposit)))   
        parking = item["car_bays"]   
        if parking != 0:
            item_loader.add_value("parking", True)
        elif parking == 0:
            item_loader.add_value("parking", False)

        item_loader.add_value("images", item["images"])      
        item_loader.add_value("landlord_name", item["property_manager_name"])
        item_loader.add_value("landlord_phone", item["property_manager_mobile"])
        item_loader.add_value("landlord_email", item["property_manager_email"])

        yield item_loader.load_item()
    
def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None