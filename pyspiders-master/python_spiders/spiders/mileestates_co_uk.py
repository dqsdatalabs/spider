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
    name = 'mileestates_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.mileestates.co.uk/search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&minprice=&maxprice=&bedrooms="]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='relative']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            rooms = "".join(item.xpath(".//div[@class='itemRooms']//text()").getall())
            yield Request(follow_url, callback=self.populate_item, meta={'rooms': rooms})
            
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Mileestates_Co_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])

        f_text = " ".join(response.xpath("//div[@class='descriptionsColumn']//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        rooms = response.meta.get('rooms')
        if rooms:
            room_count = rooms.split(",")[0].strip()
            bathroom_count = rooms.split(",")[-1].strip()
            if room_count != "0":
                item_loader.add_value("room_count", room_count)
            
            if bathroom_count != "0":
                item_loader.add_value("bathroom_count", bathroom_count)
        
        address = response.xpath("//h1[@class='fdPropName']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-2].strip())
        zipcode=response.xpath("//title/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(",")[-1])
        
        rent = "".join(response.xpath("//h2[@class='fdPropPrice']/div/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.split("PCM")[0].strip().replace(",",""))
        
        
        desc = " ".join(response.xpath("//div[@class='descriptionsColumn']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[contains(@id,'property-photos')]//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        square_meters = response.xpath("//li[contains(.,'sq ft')]//text()").get()
        if square_meters:
            square_meters = square_meters.split("sq ft")[0].strip().split(" ")[-1].replace(",","")
            sqm = str(int(int(square_meters)* 0.09290304))
            item_loader.add_value("square_meters", sqm)
        
        floor = response.xpath("//li[contains(.,'floor')]//text()").get()
        if floor:
            floor = floor.split("floor")[0].strip().split(" ")[-1]
            if "Wood" not in floor:
                item_loader.add_value("floor", floor.capitalize())
        
        energy_label = response.xpath("//li[contains(.,'EPC')]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip().split(" ")[-1])
        elif "epc rating" in desc.lower():
            energy_label = desc.lower().split("epc rating")[1].replace(":","").strip().split(" ")[0]
            item_loader.add_value("energy_label", energy_label.upper())
        
        deposit = response.xpath("//div[@class='descriptionsColumn']//text()[contains(.,'deposit')]").get()
        if deposit:
            deposit = deposit.split("week")[0].strip().split(" ")[-1]
            price = int(rent.split("PCM")[0].split("£")[1].strip().replace(",",""))/4
            item_loader.add_value("deposit", int(float(int(deposit)*price)))
        
        floor_plan_images = response.xpath("//a[contains(.,'Floor')]/@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        furnished = response.xpath("//li[contains(.,' furnished') or contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        balcony = response.xpath("//li[contains(.,'balcon') or contains(.,'Balcon') or contains(.,'BALCON')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking') or contains(.,'PARKING')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        from datetime import datetime
        import dateparser
        available_date = response.xpath("//li[contains(.,'Available')]//text()").get()
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            available_date = available_date.split("Available")[1].replace("!","").replace("End of","").strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        
        item_loader.add_value("landlord_name", "Mile Estate Agents")
        item_loader.add_value("landlord_phone", "020 8968 6000")
        item_loader.add_value("landlord_email", "nw10@mileestates.co.uk")
        
        
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