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
    name = 'applepropertyservices_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        formdata = {
            "area": "",
            "type": "",
            "minbaths": "",
            "salerent": "nr",
            "minbeds": "",
            "minprice": "",
            "maxprice": "",
            "PropPerPage": "12",
            "order": "low",
            "radius": "0",
            "grid": "grid",
            "search": "yes",
        }
        url = "https://www.applepropertyservices.com/search.vbhtml/properties-to-rent"
        yield FormRequest(
            url,
            callback=self.parse,
            formdata=formdata,
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='property-thumb-info-image']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            formdata = {
                "area": "",
                "type": "",
                "minbaths": "",
                "salerent": "nr",
                "minbeds": "",
                "minprice": "",
                "maxprice": "",
                "PropPerPage": "12",
                "order": "low",
                "radius": "0",
                "grid": "grid",
                "search": "yes",
                "links": str(page),
                }
            url = "https://www.applepropertyservices.com/search.vbhtml/properties-to-rent"
            yield FormRequest(
                url,
                callback=self.parse,
                formdata=formdata,
                meta={"page":page+1}
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Applepropertyservices_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)

        f_text = " ".join(response.xpath("//div[@class='col-md-12']//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = " ".join(response.xpath("//ul[@class='key-features']/li/text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="M_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='fullprice2']/text()", get_num=True, input_type="M_XPATH", replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p[@class='photos-pad']/text()[contains(.,'Bedroom')]", input_type="M_XPATH", split_list={"Bedroom":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//p[@class='photos-pad']/text()[contains(.,'Bathroom')]", input_type="M_XPATH", split_list={"Bathroom":0, "/":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//ul[@class='key-features']/li/text()[contains(.,'parking') or contains(.,'PARKING') or contains(.,'Parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//ul[@class='key-features']/li/text()[contains(.,'balcony') or contains(.,'BALCONY')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//ul[@class='key-features']/li/text()[contains(.,'balcony') or contains(.,'FURNISHED')]", input_type="F_XPATH", tf_item=True)
        
        desc = " ".join(response.xpath("//h2[contains(.,'Summary')]/parent::div//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="substring-after(//p[contains(.,'Reference:')]/text(),':')", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'Lng')]/text()", input_type="M_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'Lng')]/text()", input_type="M_XPATH", split_list={"LatLng(":1, ",":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='fotorama']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//div[@id='floorplan']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//img/@src[contains(.,'epc')]", input_type="F_XPATH", split_list={"epc1=":1, "&":0})
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//ul[@class='key-features']/li/text()[contains(.,'AVAILABLE')]", input_type="F_XPATH", split_list={"AVAILABLE":1})
        
        floor = response.xpath("//ul[@class='key-features']/li/text()[contains(.,'FLOO')]").get()
        if floor:
            item_loader.add_value("floor", floor.split(" ")[0])
            
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="APPLE PROPERTY SERVICES", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01708 704 768", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="david@applepropertyservices.com", input_type="VALUE")
        
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None