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
import dateparser
from datetime import datetime

class MySpider(Spider):
    name = 'valeriusrentals_com'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'en'
    
    start_urls = ["https://www.valeriusrentals.com/rentals/?widget_id=2&kind=0&sf_unit_price=150&sf_unit_living_area=2"] #LEVEL-1

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//h3[@class='wpl_prp_title']"):
            p_type_info = item.xpath("./text()").get()
            follow_url = response.urljoin(item.xpath("./../@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"p_type_info":p_type_info})

        next_page = response.xpath("//a[.='Next']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
            )     
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source","Valeriusrentals_PySpider_netherlands")
        item_loader.add_value("external_link", response.url)
        p_type_info = response.meta["p_type_info"]
        if get_p_type_string(p_type_info):
            item_loader.add_value("property_type", get_p_type_string(p_type_info))
        else:
            return

        item_loader.add_css("title","h1")
        
        address = response.xpath("//h2/span/text()").get()
        if address:
            item_loader.add_value("address", address)
            
        city = response.xpath("//label[contains(.,'City')]/following-sibling::span/text()").get()
        if city:
            item_loader.add_value("city", city)
        
        zipcode = response.xpath("//label[contains(.,'Zipcode')]/following-sibling::span/text() | //label[contains(.,'Postal Code ')]/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//div[contains(@class,'detail')]/div[contains(.,'Price')]/span/text()").get()
        if rent:
            price = rent.split(" ")[0].split("€")[1].replace(".","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//div[contains(@class,'detail')]/div[contains(.,'Bedroom')]/span/text()").get()
        if room_count and room_count !="0":
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//div[contains(@class,'detail')]/div[contains(.,'Bathroom')]/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        square_meters = response.xpath("//div[contains(@class,'detail')]/div[contains(.,'Size')]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        available_date = response.xpath("//div[contains(@class,'detail')]/div[contains(.,'Available')]/span/text()").get()
        if available_date:
            if "direct" in available_date.lower():
                available_date = datetime.now().strftime("%Y-%m-%d")
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        desc = " ".join(response.xpath("//div[contains(@class,'detail')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//li[contains(@id,'gallery')]/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        balcony = response.xpath("//label[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        dishwasher = response.xpath("//label[contains(.,'Dishwasher')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        elevator = response.xpath("//label[contains(.,'Elevator')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        washing_machine = response.xpath("//label[contains(.,'Washing Machine')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        furnished = response.xpath("//label[contains(.,'Condition')]/following-sibling::span/text()").get()
        if furnished:
            if "Not" in furnished:
                item_loader.add_value("furnished", False)
            elif "Furnished" in furnished:
                item_loader.add_value("furnished", True)
        
        parking = response.xpath("//label[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        terrace = response.xpath("//label[contains(.,'Roof Deck')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        floor = response.xpath("//label[contains(.,'Floor')]/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        latitude = response.xpath("//label[contains(.,'Latitude')]/following-sibling::span/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        
        longitude = response.xpath("//label[contains(.,'Longitude')]/following-sibling::span/text()").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_value("landlord_name", "Valerius Rentals B.V.")
        item_loader.add_value("landlord_phone", "31 (0)20 891 28 20")
        item_loader.add_value("landlord_email","info@valeriusrentals.nl")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None