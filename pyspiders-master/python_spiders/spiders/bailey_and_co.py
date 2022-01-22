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
import dateparser
from word2number import w2n

class MySpider(Spider):
    name = 'bailey_and_co'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        yield Request(
            "https://www.bailey-and.co/",
            callback=self.jump,
        )

    def jump(self, response):
        view_state = response.xpath("//input[@name='__VIEWSTATE']/@value").get()
        view_state_gen = response.xpath("//input[@name='__VIEWSTATEGENERATOR']/@value").get()
        event_val = response.xpath("//input[@name='__EVENTVALIDATION']/@value").get()
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "2",
            },
            {
                "property_type" : "house",
                "type" : "1",
            },
        ]
        for item in start_urls:
            formdata = {
                "__EVENTTARGET": "ctl00$lnkSearch",
                "__EVENTARGUMENT": "",
                "__VIEWSTATE": view_state,
                "__VIEWSTATEGENERATOR": view_state_gen,
                "__EVENTVALIDATION": event_val,
                "ctl00$txtPostcode$txtTextBox": "",
                "ctl00$ddlPropertyTypeRental": item["type"],
                "ctl00$ddlLowerRangeRent": "0",
                "ctl00$ddlUpperRangeRent": "",
                "ctl00$ddlMinBedrooms": "",
                "ctl00$txtBuyPostcode$txtTextBox": "",
                "ctl00$ddlBuyProperties": "0",
                "ctl00$ddlMinBuyProperty": "",
                "ctl00$ddlMaxBuyProperty": "",
                "ctl00$ddlBuyMinBedroom": "",
            }
            api_url = "https://www.bailey-and.co/"
            yield FormRequest(
                url=api_url,
                callback=self.parse,
                formdata=formdata,
                #dont_filter=True,
                meta={
                    "property_type":item["property_type"],
                })

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='image']"):
            status = item.xpath("./img[contains(@src,'assets')]/@src").get()
            if status and "let" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Bailey_And_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("title", address.strip())
            item_loader.add_value("address", address.strip())
            zipcode = address.strip().split(" ")[-1]
            item_loader.add_value("zipcode", zipcode.strip())
            
            city = address.split(zipcode)[0].split(",")[-1].strip()
            if city:
                item_loader.add_value("city", city)
        
        rent = response.xpath("//span[contains(@class,'price')]/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("£")[1].replace(",","").strip())
        # ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="", input_type="F_PATH", split_list={"£":1}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        
        description = "".join(response.xpath("//div[contains(@class,'content_wrap')]//text()").getall())
        if description:
            desc = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", desc)

            if "sq" in desc.lower():
                square_meters = desc.lower().split("sq")[0].strip().split(" ")[-1].replace(",","")
                sqm = str(int(float(square_meters)* 0.09290304))
                item_loader.add_value("square_meters", sqm)
        
        
        features = response.xpath("//div[contains(@class,'content_wrap')]//text()[contains(.,'EPC')]").get()
        if features:
            features = features.lower()
            if "bedroom" in features:
                try:
                    room_count = features.replace("double","").split("bedroom")[0].strip().split(" ")[-1]
                    if "m2" in room_count:
                        room_count = description.split("bedroom")[0].strip().split(" ")[-1]
                    item_loader.add_value("room_count", w2n.word_to_num(room_count))
                except:
                    pass

            if "floor" in features:
                floor = features.split("floor")[0].strip().split(" ")[-1]
                if "wood" not in floor:
                    item_loader.add_value("floor", floor)
            
            if "parking" in features:
                item_loader.add_value("parking", True)
        
            if " furnished" in features:
                item_loader.add_value("furnished", True)
        
            if "epc" in features:
                energy_label = features.split("epc")[1].replace("rating","").replace(":","").strip().split(" ")[0].upper()
                item_loader.add_value("energy_label", energy_label)
                        
            if "available" in features:
                available_date = features.split("available")[1].strip().split(" ")
                if "immediately" in available_date[0]:
                    item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
                available_date = available_date[0]+ " "+ available_date[1]+" "+available_date[2]
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                    
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'Lng')]/text()", input_type="M_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'Lng')]/text()", input_type="M_XPATH", split_list={"LatLng(":1, ",":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'slideshow')]//a//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//img[contains(@alt,'floorplan')]/@src", input_type="M_XPATH")
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Bailey & Co.", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0191 281 2305", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@bailey-and.co", input_type="VALUE")
        
        
        yield item_loader.load_item()
