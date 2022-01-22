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

class MySpider(Spider):
    name = 'andrewnunnassociates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://andrewnunnassociates.co.uk/let/property-to-let//page/1"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='module-content']/a"):
            status = item.xpath(".//div[@class='pstatus']/span/text()").get()
            if status and ("agree" in status.lower() or status.lower() == "let"):
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://andrewnunnassociates.co.uk/let/property-to-let//page/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Andrewnunnassociates_Co_PySpider_united_kingdom", input_type="VALUE")
        item_loader.add_value("external_link", response.url.split("?")[0])
        f_text = " ".join(response.xpath("//div[contains(@class,'property-details')]/p//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//div[contains(@class,'main-property-features')]//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: 
            return
        item_loader.add_value("external_id", response.url.split("property/")[1].split("/")[0])
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[contains(@class,'large-12')]/h1//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[contains(@class,'large-12')]/h1//text()", input_type="M_XPATH", split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[contains(@class,'large-12')]/h1//text()", input_type="M_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(@class,'value')]/text()", input_type="F_XPATH", get_num=True, per_week=True, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'bedroom')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(.,'bathroom')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
                
        desc = " ".join(response.xpath("//div/h3[contains(.,'Desc')]/..//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        square_meters = response.xpath("//div/i/parent::div[contains(.,'sq')]//text()").get()
        if square_meters:
            square_meters = square_meters.split("sq")[0].strip().split(" ")[-1]
        elif "sq " in desc:
            square_meters = desc.split("sq")[0].strip().split(" ")[-1]
        
        if square_meters and square_meters.isdigit():
            sqm = str(int(int(square_meters)* 0.09290304))
            item_loader.add_value("square_meters", sqm)
            
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={'lat": "':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={'lng": "':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'lightboxcarousel')]//@data-image-src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//a[contains(@class,'floorplan')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div/i/parent::div[contains(.,'terrace')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div/i/parent::div[contains(.,'Furnished') or contains(.,'FURNISHED')]//text()", input_type="F_XPATH", tf_item=True)
        
        parking = response.xpath("//div/i/parent::div[contains(.,'parking') or contains(.,'Parking')]//text()").get()
        if parking and "No" not in parking:
            item_loader.add_value("parking",True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div/i/parent::div[contains(.,'Available') or contains(.,'AVAILABLE')]//text()", input_type="F_XPATH", replace_list={"Available":"", "AVAILABLE":""})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div/i/parent::div[contains(.,' floor') or contains(.,'FLOOR')][not(contains(.,'Wood'))]//text()", input_type="F_XPATH", split_list={" ":0})
        
        if "EPC" in desc:
            energy_label = desc.split("EPC")[1].split(".")[0].strip().split(" ")[-1]
            if "TBC" not in energy_label:
                item_loader.add_value("energy_label", energy_label)
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="ANDREW NUNN & Associates", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="020 8995 1600", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="lettings@andrewnunnassociates.co.uk", input_type="VALUE")

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