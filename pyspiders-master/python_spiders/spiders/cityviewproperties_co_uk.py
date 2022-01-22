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
from word2number import w2n
from datetime import datetime
import dateparser

class MySpider(Spider):
    name = 'cityviewproperties_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["http://cityviewproperties.co.uk/pages/PropertyViewList.aspx?cid=2&ddlPropertyType=&minBedrooms=undefined&maxBedrooms=undefined&SaleminPrice=undefined&SalemaxPrice=undefined&LettingMinPrice=undefined&LettingMaxPrice=undefined"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'propertyListViewRow')]/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Cityviewproperties_Co_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)

        f_text = " ".join(response.xpath("//span[@id='MainContent_lblPropertyType']/text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//span[@id='MainContent_lblTitle']/text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        
        title = response.xpath("//span[contains(@id,'Title')]/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        address = response.xpath("//span[contains(@id,'Address')]/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(" ")[-1]
            city = address.split(",")[-1].split(zipcode)[0].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            
        
        rent = "".join(response.xpath("//span[contains(@id,'trRent')]//text()").getall())
        price = ""
        if rent:
            price = rent.split("£")[1].split("(")[0].replace(" ","")
            item_loader.add_value("rent", int(price)*4)
            item_loader.add_value("currency", "GBP")
        
        room_count = "".join(response.xpath("//span[contains(.,'Beds')]/parent::div/following-sibling::div/span//text()").getall())
        if room_count:
            room_count = room_count.strip()
            if room_count != "0":
                item_loader.add_value("room_count", room_count)
            elif prop_type == "studio":
                item_loader.add_value("room_count", "1")
        
        bathroom_count = "".join(response.xpath("//span[contains(.,'Baths')]/parent::div/following-sibling::div/span//text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("lat = '")[1].split("'")[0]
            longitude = latitude_longitude.split("long = '")[1].split("'")[0]     
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        desc = " ".join(response.xpath("//span[contains(@id,'Description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "weeks deposit" in desc:
            deposit = desc.split("weeks deposit")[0].strip().split(" ")[-1]
            deposit = w2n.word_to_num(deposit) * int(price)
            item_loader.add_value("deposit", deposit)
        
        if "floor" in desc.lower():
            floor = desc.lower().split("floor")[0].strip().split(" ")[-1]
            if "wood" not in floor and "lami" not in floor and "carpe" not in floor and "yes" not in floor:
                item_loader.add_value("floor", floor.replace("-",""))
            
        images = [x for x in response.xpath("//div[@id='thumbs']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        
        if "available immediately" in desc.lower() or "available now" in desc.lower():
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        elif "Available from the" in desc:
            available_date = desc.split("Available from the")[1].split("!")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        item_loader.add_value("landlord_name", "CITYVIEW PROPERTIES")
        item_loader.add_value("landlord_phone", "020 8980 3200")
        item_loader.add_value("landlord_email", "info@cityviewproperties.co.uk")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "etage" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None