# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re
from datetime import datetime

class MySpider(Spider):
    name = 'alexanders_uk_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://alexanders-uk.com/let/property-to-let/"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='result_itemwrapper']/a"):
            status = item.xpath("./div/div/text()").get()
            if status and ("agree" in status.lower() or status.lower() == "let"):
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://alexanders-uk.com/let/property-to-let/page/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Alexanders_Uk_PySpider_united_kingdom", input_type="VALUE")
        item_loader.add_value("external_link", response.url.split("?")[0])
        f_text = response.url
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//h2[@class='info_title']//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: 
            return
        item_loader.add_value("external_id", response.url.split("_")[1].split("/")[0])
        
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[contains(@id,'description')]//h2[@class='info_title']//text()", input_type="M_XPATH", replace_list={"\r":"", "\n":""})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='info_address']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h2/span[contains(@class,'currencyvalue')]/text()", input_type="F_XPATH", get_num=True, per_week=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        
        city = response.xpath("//div[@class='info_address']//text()").get()
        if city:
            zipcode = city.strip().split(" ")[-1]
            if not zipcode.isalpha():
                item_loader.add_value("zipcode", zipcode.strip())
                city = city.split(zipcode)[0].strip().strip(",").split(",")[-1]
                item_loader.add_value("city", city.strip())
            else:
                item_loader.add_value("city", zipcode.strip())
        
        room_count = response.xpath("//div[@class='prop_itemwrapper']/span[1]//text()").get()
        if "Studio" in room_count:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="1", input_type="VALUE")
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='prop_itemwrapper']/span[1]//text()", input_type="F_XPATH", get_num=True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@class='prop_itemwrapper']/span[2]//text()", input_type="F_XPATH", get_num=True)
        
        desc = " ".join(response.xpath("//div[@class='description_wrapper']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            
        square_meters = response.xpath("//div[@class='feature'][contains(.,'Sq')]/text()").get()
        if square_meters:
            ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[@class='feature'][contains(.,'Sq')]/text()", input_type="F_XPATH", get_num=True, split_list={"Sq":0, "-":1}, replace_list={",":""}, sq_ft=True)
        elif "sq m" in desc.lower():
            square_meters = desc.lower().split("sq m")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        if "available immediately" in desc or "Available now" in desc:
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        
        if "floor" in desc.lower():
            floor = desc.lower().split("floor")[0].strip().split(" ")[-1].strip("-")
            if "-" in floor:
                item_loader.add_value("floor", floor.split("-")[1])
            elif "wood" not in floor:
                item_loader.add_value("floor", floor)
                
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'imageviewer')]//@data-image-src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//img[@id='fpimg']/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={'lat": "':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={'lng": "':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[@class='feature'][contains(.,'Balcony')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Alexanders Property Consultants", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="020 7431 0666", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="nw6@alexanders-uk.com", input_type="VALUE")

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("flat" in p_type_string.lower() or "terrace" in p_type_string.lower() or "apartment" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    elif p_type_string and "suite" in p_type_string.lower():
        return "room"
    else:
        return None