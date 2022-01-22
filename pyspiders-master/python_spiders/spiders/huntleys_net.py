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
    name = 'huntleys_net'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    
    def start_requests(self):
        formdata = {
            "MaxPrice": "Max Price",
            "Beds": "Min Bedrooms",
            "Sort": "Sort By",
            "Student": "Non-student",
            "submit": "Search",
        }
        url = "https://huntleys.net/find-a-home/"
        yield FormRequest(
            url,
            callback=self.parse,
            formdata=formdata,
        )

        formdata_student = {
            "MaxPrice": "Max Price",
            "Beds": "Min Bedrooms",
            "Sort": "Sort By",
            "Student": "Student",
            "submit": "Search",
        }
        url = "https://huntleys.net/find-a-home/"
        yield FormRequest(
            url,
            callback=self.parse,
            formdata=formdata_student,
            meta={"p_type":"student_apartment"}
        )

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//img[@class='propimgover']/../@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"p_type":response.meta.get("p_type", "")})  
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-2])
        p_type = response.meta["p_type"]
        if p_type:
            item_loader.add_value("property_type", p_type)
        else:
            f_text = " ".join(response.xpath("//div[contains(@class,'entry-title nott col-padding det-pad mt-3 py-0')]//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Huntleys_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='entry-meta']/following-sibling::text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='entry-meta']//i[@class='icon-bed']/../text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@class='entry-meta']//i[@class='icon-bath']/../text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='entry-price']/text()", input_type="F_XPATH", split_list={".":0}, get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li[contains(.,'Available')]/text()", input_type="F_XPATH", lower_or_upper=0, replace_list={"available":"", "from":""})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[contains(text(),'Deposit')]/text()", input_type="F_XPATH", split_list={".":0}, get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'gallery-images')]//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//div[contains(@class,'property-floorplan')]//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="pets_allowed", input_value="//li[contains(.,'Pets considered')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking') or contains(.,'parking')] | //h4[contains(.,'Parking')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//h1/../text()[contains(.,'Terrace') or contains(.,'terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//li[contains(.,'Dishwasher') or contains(.,'dishwasher')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[contains(.,' floor')]//text()", input_type="F_XPATH", split_list={" floor":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Huntleys", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01509320320", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="let@huntleys.net", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Unfurnished') or contains(.,'unfurnished')]", input_type="F_XPATH", tf_item=True, tf_value=False)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished') or contains(.,'furnished')]", input_type="F_XPATH", tf_item=True, tf_value=True)
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[@class='epc']/img/@src", input_type="F_XPATH", split_list={"epc.png?":-1, ",":0, "currentenergy=":-1, "&":0})

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
    else:
        return None