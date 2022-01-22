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

class MySpider(Spider):
    name = 'bmvmakelaars_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://bmvmakelaars.nl/api/properties/available.json?nocache=1610001367522"]

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data["objects"]:
            if "onder optie" not in item["availability_status"].lower() and "rent" in item["buy_or_rent"].lower():
                follow_url = response.urljoin(item["url"])
                p_type = item["house_type"]
                if get_p_type_string(p_type):
                    prop_type = get_p_type_string(p_type)
                else:
                    continue
                
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":prop_type, "item": item})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Bmvmakelaars_PySpider_netherlands")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        id=response.url
        if id:
            id=id.split("-")[-1]
            item_loader.add_value("external_id",id)
        item = response.meta.get("item")
        item_loader.add_value("title", item["title"])
        
        item_loader.add_value("address", item["street_name"]+","+item["place"])
        item_loader.add_value("city", item["place"])
        item_loader.add_value("zipcode", item["zip_code"])
        item_loader.add_value("rent", item["rent_price"])
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("room_count", item["amount_of_bedrooms"])
        item_loader.add_value("square_meters", item["usable_area_living_function"])
        item_loader.add_value("latitude", item["latitude"])
        item_loader.add_value("longitude", item["longitude"])
        
        bathroom_count = response.xpath("//th[contains(.,'badkamer')]/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(" ")[0])
        
        desc = " ".join(response.xpath("//div[contains(@class,'description')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//figure[contains(@class,'media__item')]//img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//button[contains(@class,'floormap')]//img/@data-src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        energy_label = response.xpath("//th[.='Energielabel']/following-sibling::td/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        floor = response.xpath("//th[contains(.,'Aantal woonlagen')]/following-sibling::td/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        utilities = response.xpath("//th[contains(.,'Service')]/following-sibling::td/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[1].strip())
        
        furnished = item["featured"]
        if furnished:
            item_loader.add_value("furnished", True)
        elif not furnished:
            item_loader.add_value("furnished", False)
        
        parking = "".join(response.xpath("//th[contains(.,'Garage')]/following-sibling::td//text()").getall())
        if parking:
            if "geen" in parking:
                item_loader.add_value("parking", False)
        
        item_loader.add_value("landlord_name", "BMV MAKELAARS")
        item_loader.add_value("landlord_phone", "026 355 21 00")
        item_loader.add_value("landlord_email", "secretariaat@bmvmakelaars.nl")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woonhuis" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None