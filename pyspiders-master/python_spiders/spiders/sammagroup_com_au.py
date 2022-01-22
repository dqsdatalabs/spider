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
from datetime import datetime
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'sammagroup_com_au'
    execution_type='testing'
    country='australia'
    locale='en' 
    start_urls = ["https://www.sammagroup.com.au/realestate/for-lease/"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='item active']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        f_text = "".join(response.xpath("//div[@class='wpestate_property_description']//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = "".join(response.xpath("//h1//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return
        item_loader.add_value("external_source", "Sammagroup_Com_PySpider_australia")   
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())   
            if "unfurnished" in title.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in title.lower():
                item_loader.add_value("furnished", True)
            
        external_id = response.xpath("//div[strong[.='Property Id :']]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip()) 
        room_count = response.xpath("//div[strong[.='Bedrooms:']]/text()[.!=' 0']").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())  
        elif "studio" in item_loader.get_collected_values("property_type"):
            item_loader.add_value("room_count", "1")  

        item_loader.add_xpath("bathroom_count", "//div[strong[.='Bathrooms:']]/text()")   
        item_loader.add_xpath("deposit", "//div[strong[.='bond_amount:']]/text()[.!=' 0']")

        address = response.xpath("//div[@class='property_categs']/text()[normalize-space()]").get()
        if address:
            item_loader.add_value("address", address.strip())
        city = response.xpath("//div[strong[.='Suburb:']]/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        zipcode = "".join(response.xpath("//div[strong[.='State/County:']]/text() | //div[strong[.='Zip:']]/text()").getall())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        parking = response.xpath("//ul[li[@class='first_overview']]/li[contains(.,'Garage')]/text()").get()
        if parking:
            if parking.strip() =="0 Garages":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        
        rent = response.xpath("//div[strong[.='Price:']]/text()").get()
        if rent:
            rent = rent.split("$")[-1].strip().split("p")[0].replace(",","")
            item_loader.add_value("rent", int(float(rent))*4)
        item_loader.add_value("currency", "AUD")
        desc = " ".join(response.xpath("//div[@class='wpestate_property_description']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        available_date = response.xpath("//ul[li[contains(.,'Available Date:')]]/li[2]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        images = [x for x in response.xpath("//div[@id='property_slider_carousel']//a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        floor_plan = [x for x in response.xpath("//div[@class='floor_image']//a/img/@src").getall()]
        if floor_plan:
            item_loader.add_value("floor_plan_images", floor_plan)
        item_loader.add_value("landlord_name", "Samma Group")
        item_loader.add_value("landlord_phone", "1300 141 888")
        item_loader.add_value("landlord_email", "enquiries@sammagroup.com.au")
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "unit" in p_type_string.lower() or "home" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    else:
        return None