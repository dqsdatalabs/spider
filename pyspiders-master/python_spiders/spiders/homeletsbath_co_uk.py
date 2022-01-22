# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'homeletsbath_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://p8a89m3g3g.execute-api.eu-west-2.amazonaws.com/prod/api/properties/search/"]
    external_source='Homeletsbath_Co_PySpider_united_kingdom'

    headers ={
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
    }
    # 1. FOLLOWING
    # 2. SCRAPING level 2
    def parse(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)

        data = json.loads(response.body)
        for item in data:
            p_type = item["type"]

            external_id=item["agentRef"]
            if external_id:
                item_loader.add_value("external_id", external_id)

            if get_p_type_string(p_type):
                p_type = get_p_type_string(p_type)
                item_loader.add_value("property_type", p_type)

            f_type = "Student" if "student" in p_type.lower() else "Residential"
            follow_url = f"https://www.homeletsbath.co.uk/propertySearch?agentRef={external_id}&type={f_type}"
            if follow_url:
                item_loader.add_value("external_link", follow_url)

            bathroom_count=item["bathrooms"]
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count)
                
            room_count=item["bedrooms"]
            if room_count:
                item_loader.add_value("room_count", room_count)

            latitude=item["latitude"]
            if latitude:
                item_loader.add_value("latitude", latitude)

            longitude=item["longitude"]
            if longitude:
                item_loader.add_value("longitude", longitude)

            price=item["price"]
            if price:
                item_loader.add_value("rent", price)
            item_loader.add_value("currency", "GBP")

            description=item["description"]
            if longitude:
                item_loader.add_value("description", description)
            
            furnished=item["furnished"]
            if furnished and "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)

            available=item["available"]
            if available:
                item_loader.add_value("available_date", available)
        
            images=item["leadImage"]
            if images:
                item_loader.add_value("images", images)

            item_loader.add_value("landlord_name", "HomeLets")
            item_loader.add_value("landlord_phone", "01225 484811")
            item_loader.add_value("landlord_email", "info@homeletsbath.co.uk") 

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
    elif p_type_string and "room" in p_type_string.lower():
        return "room"
    else:
        return None