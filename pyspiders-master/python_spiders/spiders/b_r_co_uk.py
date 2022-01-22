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

class MySpider(Spider):
    name = 'b_r_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="B_R_Co_PySpider_united_kingdom"
    custom_settings = {"PROXY_TR_ON": True}

    def start_requests(self):
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "flat",
                "url" : "https://www.dexters.co.uk/property-lettings/flats-available-to-rent-in-london",
            },
            {
                "property_type" : "house",
                "type" : "house",
                "url" : "https://www.dexters.co.uk/property-lettings/houses-available-to-rent-in-london",
            },
        ]
        for item in start_urls:
            formdata = {
                "location": "",
                "price[]": "",
                "price[]": "",
                "building": item["type"],
                "search_type": "L",
                "order_dir": "ASC",
                "department": "",
                "includesold_hidden": "1",
                "option": "com_startek",
                "view": "results",
                "task": "search",
                "order": "price",
                "Itemid": "116",
            }
            yield FormRequest(
                url=item["url"],
                callback=self.parse,
                formdata=formdata,
                #dont_filter=True,
                meta={
                    "property_type":item["property_type"],
                    "base_url":item["url"]
                })

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='result-image test']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url + f"/page-{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={
                    "property_type":response.meta["property_type"],
                    "page":page+1,
                    "base_url":base_url,
                })
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        rent = response.xpath("//span[@class='price']/text()").get()
        if rent:
            rent = rent.split("-")[-1].replace("£","").replace("Pw","").replace("/","").replace(",","").strip()
            item_loader.add_value("rent",int(rent)*4)

        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title//text()")
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value=response.url, input_type="VALUE", split_list={"/":-1})
        item_loader.add_value("external_source","B_R_Co_PySpider_united_kingdom")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//header/h1[contains(@class,'title')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//header/h1[contains(@class,'title')]//text()", input_type="M_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//header/h1[contains(@class,'title')]//text()", input_type="M_XPATH",split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[@class='Bathroom']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent_string", input_value="//span[contains(@class,'price-q')]/text()", input_type="F_XPATH", get_num=True, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[@class='Bathrooms']/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        # ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lat':":1,",":0})
        # ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lng':":1, ',':0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//div[contains(@id,'floorplan-modal')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//li[@class='slide']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[@class='contacts']/h2/a/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@class='contacts']//span[@class='phone']/a/text()", input_type="F_XPATH")
        longitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if longitude:
            longitude=longitude.split("lng")[1].split(",")[0]
            if longitude:
                item_loader.add_value("longitude",longitude)
        latitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude:
            latitude=latitude.split("lat")[1].split(",")[0]
            if latitude:
                item_loader.add_value("latitude",latitude)
        desc = "".join(response.xpath("//div[contains(@class,'section-entry')]//text()").getall())
                
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            
            from datetime import datetime
            if "available now" in desc.lower() or "available immediately" in desc.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))

            if "EPC Rating" in desc:
                energy_label = desc.split("EPC Rating")[1].split(".")[0].strip()
                item_loader.add_value("energy_label", energy_label)
            
            if "floor" in desc:
                floor = desc.split("floor")[0].strip().split(" ")[-1]
                not_list = ["lami","carpe","each","wood","sepera", "quali", "spacious","place","tile"]
                status = True
                for i in not_list:
                    if i in floor.lower():
                        status = False
                if status:
                    item_loader.add_value("floor", floor.replace("-","").upper())
                    
            if "sq ft" in desc.lower():
                square_meters = desc.lower().split("sq ft")[0].strip().split(" ")[-1].replace("(","").replace(",","")
                sqm = str(int(int(float(square_meters))* 0.09290304))
                item_loader.add_value("square_meters", sqm)

        room_count = response.xpath("//li[contains(@class,'Bedroom')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        elif response.xpath("//li[contains(@class,'Reception')]//text()").get():
            room_count = response.xpath("//li[contains(@class,'Reception')]//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        elif "studio" in desc.lower():
            item_loader.add_value("room_count", "1")
        
        if "studio" in desc.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta["property_type"])

        
            
        
        
        yield item_loader.load_item()
