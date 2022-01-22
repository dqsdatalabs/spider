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
import dateparser
from datetime import datetime
class MySpider(Spider):
    name = 'nationaalgrondbezit_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://api.nationaalgrondbezit.nl/wp-json/api/components/post-type/property"]

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data["data"]:
            if "verhuurd" in item["fields"]["property_status"]:
                continue
            if "property_characteristics_type" in item["fields"]:
                p_type = item["fields"]["property_characteristics_type"]
            else:
                continue
            follow_url = f"https://nationaalgrondbezit.nl/huuraanbod/undefined/{item['slug']}"
            yield Request(follow_url, callback=self.populate_item, meta={"p_type":p_type,"item":item})
             
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        f_text = response.meta["p_type"]
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        item_loader.add_value("external_source", "Nationaalgrondbezit_PySpider_netherlands")
        item = response.meta["item"]
        item_loader.add_value("title", str(item["title"])) 
        zipcode = item["fields"]["property_postal_code"]
        city = item["fields"]["property_city"]
        address = item["fields"]["property_formated_address"]
        lat = item["fields"]["property_lat"]
        lng = item["fields"]["property_long"]

        item_loader.add_value("latitude", str(lat))
        item_loader.add_value("longitude", str(lng))       
        item_loader.add_value("address", address+ ", "+city)
        item_loader.add_value("city", str(city))
        item_loader.add_value("zipcode", str(zipcode))

        if "property_rent_price" in item["fields"]:
            rent = item["fields"]["property_rent_price"]
            item_loader.add_value("rent", int(float(rent)))
        item_loader.add_value("currency", "EUR")

        if "property_dimensions_living_area" in item["fields"]:
            square_meters = item["fields"]["property_dimensions_living_area"]
            item_loader.add_value("square_meters", int(float(square_meters)))

        if "property_service_costs" in item["fields"]:
            utilities = item["fields"]["property_service_costs"]
            item_loader.add_value("utilities", int(float(utilities)))

        if "property_security_deposit" in item["fields"]:
            deposit = item["fields"]["property_security_deposit"]
            item_loader.add_value("deposit", int(float(deposit)))
            
        if "property_layouts_floors" in item["fields"]:
            item_loader.add_value("floor",item["fields"]["property_layouts_floors"])

        if "property_characteristics_energy_label" in item["fields"]:
            item_loader.add_value("energy_label",item["fields"]["property_characteristics_energy_label"])
        
        if "property_layouts_bathrooms" in item["fields"]:
            item_loader.add_value("bathroom_count", item["fields"]["property_layouts_bathrooms"])
        
        if "property_layouts_bedrooms" in item["fields"]:
            item_loader.add_value("room_count", item["fields"]["property_layouts_bedrooms"])
        
        if item["fields"]["property_images"]:
            images = [response.urljoin(x["property_images_image"]) for x in item["fields"]["property_images"]]
            if images:
                item_loader.add_value("images", images)
         
        desc = " ".join(response.xpath("//div[contains(@class,'description-container')]/div/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        if "property_layouts_outdoor_type" in item["fields"]:
            attr = item["fields"]["property_layouts_outdoor_type"]
            if "balkon" in attr:
                item_loader.add_value("balcony",True)
            if "terras" in attr:
                item_loader.add_value("terrace",True)

        item_loader.add_value("landlord_name", "Nationaal Grondbezit")
        item_loader.add_value("landlord_email", "info@nagron.nl")
        item_loader.add_value("landlord_phone", "+31 (0) 10 24 26 100")

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None