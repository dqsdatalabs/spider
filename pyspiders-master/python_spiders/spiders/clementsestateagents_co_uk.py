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
    name = 'clementsestateagents_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.clementsestateagents.co.uk//search?saletype=rent&minbedrooms=-1&maxbedrooms=99999999&minprice=0&maxprice=99999999&location=&sort=true"]
    external_source = "Clementsestateagents_Co_PySpider_united_kingdom"
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@id='searchresults']/a"):
            status = "".join(item.xpath(".//div[@class='image']/div/div/text()").getall())
            if status and ("agreed" in status.lower() or status.strip().lower() == "let"):
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        f_text = " ".join(response.xpath("//h2//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//div[@id='tabs-1']/p//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return
        item_loader.add_value("external_source", self.external_source)


        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value=response.url, input_type="VALUE", split_list={"lettings/":1, "/":0})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@id='tabs-1']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='right']//h2[contains(.,'bedroom')]/text()", input_type="F_XPATH", split_list={"bedroom":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='priceinfo']/text()", input_type="F_XPATH", split_list={"£":-1}, replace_list={",":""}, get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li[contains(.,'AVAILABLE')]/text()", input_type="F_XPATH", lower_or_upper=0, replace_list={"available":"", "early":"", "!":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='propslider']//a/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//div[@id='tabs-4']/img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//div[@id='tabs-2']//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//div[@id='tabs-2']//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[contains(text(),'Floor') or contains(text(),'FLOOR') or contains(.,'floor')]/text()", input_type="F_XPATH", lower_or_upper=0, split_list={"floor":0}, get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking') or contains(.,'PARKING') or contains(.,'parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcony') or contains(.,'BALCONY')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Lift') or contains(.,'LIFT')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Clements Estate Agents", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01442 214151", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="lettings@clementsestateagents.co.uk", input_type="VALUE")

        if response.xpath("//li[contains(.,'Bathroom')]").get(): item_loader.add_value("bathroom_count", "1")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None