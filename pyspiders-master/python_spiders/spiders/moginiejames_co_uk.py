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
    name = 'moginiejames_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://www.moginiejames.co.uk/search.ljson?channel=lettings&fragment=page-1"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        data = json.loads(response.body)
        for item in data["properties"]:
            p_type = item["property_type"]

            
            address = item["display_address"] if "display_address" in item.keys() else None
            room_count = str(item["bedrooms"]) if "bedrooms" in item.keys() else None
            bathroom_count = str(item["bathrooms"]) if "bathrooms" in item.keys() else None
            external_id = str(item["property_id"]) if "property_id" in item.keys() else None
            latitude = str(item["lat"]) if "lat" in item.keys() else None
            longitude = str(item["lng"]) if "lng" in item.keys() else None

            if get_p_type_string(p_type):
                p_type = get_p_type_string(p_type)
            else:
                continue
            follow_url = response.urljoin(item["property_url"])
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":p_type, "address":address, "room_count":room_count, "bathroom_count":bathroom_count,
                    "external_id":external_id, "latitude":latitude, "longitude": longitude})
        
        if data["pagination"]["has_next_page"]:
            p_url = f"https://www.moginiejames.co.uk/search.ljson?channel=lettings&fragment=page-{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])

        from python_spiders.helper import ItemClear

        
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Moginiejames_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value=response.meta["external_id"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value=response.meta["address"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'description')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value=response.meta["room_count"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value=response.meta["bathroom_count"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='property-detail__summary-price']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li[contains(.,'Available')]/text()", input_type="F_XPATH", lower_or_upper=0, replace_list={"available":"", "mid":"", "mid/late":"", "date:":"", "from the":"", "immediately":"now", "end of": ""})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li[contains(.,'Deposit')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'property-detail__gallery')]/img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//a[contains(.,'Floor Plan')]/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value=response.meta["latitude"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value=response.meta["longitude"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//li[contains(.,'EPC Rating')]/text()", input_type="F_XPATH", split_list={"-":-1, ":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking') or contains(.,'parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcony') or contains(.,'balcony')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrace') or contains(.,'terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//li[contains(.,'Washing Machine')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="pets_allowed", input_value="//li[contains(.,'Pets: NO')]", input_type="F_XPATH", tf_item=True, tf_value=False)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Unfurnished') or contains(.,'unfurnished')]", input_type="F_XPATH", tf_item=True, tf_value=False)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished') or contains(.,'furnished')]", input_type="F_XPATH", tf_item=True, tf_value=True)
        
        if response.xpath("//dt[contains(.,'Staff')]/following-sibling::dd/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//dt[contains(.,'Staff')]/following-sibling::dd/text()", input_type="F_XPATH")
        else: item_loader.add_value("landlord_name", "Moginie James")
        
        if response.xpath("//dt[contains(.,'Tel')]/following-sibling::dd/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//dt[contains(.,'Tel')]/following-sibling::dd/text()", input_type="F_XPATH")
        else: item_loader.add_value("landlord_phone", "029 20 730 883")
        
        if response.xpath("//dt[contains(.,'Email') and contains(@class,'staff')]/following-sibling::dd/a/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//dt[contains(.,'Email') and contains(@class,'staff')]/following-sibling::dd/a/text()", input_type="F_XPATH")
        else: item_loader.add_value("landlord_email", "info@moginiejames.co.uk")

        city = ""
        zipcode = ""
        address = response.meta["address"]
        if address:
            if len(address.split(",")) == 2:
                city = address.split(",")[-1].strip()
            elif len(address.split(",")) >= 3:
                city = address.split(",")[-1].strip()
                if not city.replace(" ","").isalpha():
                    zipcode = address.split(",")[-1].strip()
                    city = address.split(",")[-2].strip()
      
            
            if city !="":
                item_loader.add_value("city", city)
            if zipcode !="":
                item_loader.add_value("zipcode", zipcode)
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None