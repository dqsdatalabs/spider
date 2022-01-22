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
    name = 'frankharris_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'   
    
    start_urls = ["https://www.frankharris.co.uk/search.ljson?channel=lettings&fragment="]


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        data = json.loads(response.body)
        seen = False
        try:
            for item in data["properties"]:
                if get_p_type_string(item["tags"]):
                    yield Request(
                        response.urljoin(item['property_url']), 
                        callback=self.populate_item, 
                        meta={
                            "property_type":get_p_type_string(item["tags"]),
                            "data": item
                        }
                    )
                seen = True
        except:
            pass

        if page == 2 or seen:
            f_url = f"https://www.frankharris.co.uk/search.ljson?channel=lettings&fragment=page-{page}"
            yield Request(f_url, callback=self.parse, meta={"page":page+1})

            

    # 2. SCRAPING level 2
    def populate_item(self, response): 
        item_loader = ListingLoader(response=response)        
        property_type = " ".join(response.xpath("//div[@class='breadcrumb']//span[@class='breadcrumb_last']/text()").getall())
        item_loader.add_value("property_type", response.meta.get("property_type"))

        data = response.meta.get('data')

        item_loader.add_value("external_link", response.url)
        external_id=response.url
        if external_id:
            item_loader.add_value("external_id",external_id.split("properties/")[-1].split("/")[0])
        item_loader.add_value("external_source", "Frankharris_Co_PySpider_united_kingdom")
        title = response.xpath("//title/text()").get()
        item_loader.add_value("title", title)
        city = data["town"]
        city2 = data["locality"]
        street = data["road_name"]
        postcode = data["postcode"]
        item_loader.add_value("address", f"{city2.strip()}, {street.strip()}, {city.strip()} {postcode.strip()}")
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", postcode)

        rent = data["price_value"]
        if rent:
            rent = rent.split("£")[1].strip().replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP") 

        item_loader.add_value("description", data["description"])
        item_loader.add_value("room_count", data["bedrooms"]) 
        item_loader.add_value("bathroom_count", data["bathrooms"]) 
        

        images = [f"http:{x}" for x in data["photos"]]
        if images:
            item_loader.add_value("images", images)

        features = str(data["features"])
        if features:
            if "terrace" in features.lower():
                item_loader.add_value("terrace", True)
            if "parking" in features.lower() or "garage" in features.lower():
                item_loader.add_value("parking", True)
            if " furnished" in features.lower() or "Furnished" in features:
                item_loader.add_value("furnished", True)
            if "lift" in features.lower():
                item_loader.add_value("elevator", True)
            if "balcon" in features.lower():
                item_loader.add_value("balcony", True)
        item_loader.add_value("longitude", str(data["lat"]))
        item_loader.add_value("latitude", str(data["lng"]))
        item_loader.add_value("landlord_name", data["agency_name"])
        
        item_loader.add_value("landlord_phone", data["contact_telephone"])

        status = data["status"]
        if status and "to let" in status.lower():
            yield item_loader.load_item()
def get_p_type_string(p_type_string):
    
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower() or "detached" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    else:
        return None