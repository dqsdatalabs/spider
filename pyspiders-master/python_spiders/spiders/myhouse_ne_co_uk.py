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
from word2number import w2n

class MySpider(Spider):
    name = 'myhouse_ne_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    start_urls = ["https://www.myhouse-ne.co.uk/properties/?area=&bedrooms=&price=&availability="]

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='card_image']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[.='Next ']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Myhouse_Ne_Co_PySpider_"+ self.country)

        title = " ".join(response.xpath("//div[@class='wrapper']/h1/text()").getall())
        if get_p_type_string(title):
            item_loader.add_value("property_type", get_p_type_string(title))
        else:
            all_text = "".join(response.xpath("//div[contains(@class,'column_twofifths')]/h1/following-sibling::*//text()").getall())
            if get_p_type_string(all_text):
                item_loader.add_value("property_type", get_p_type_string(all_text))
            else:
                return

        if title:
            item_loader.add_value("title", title)
        
        rent = response.xpath("//h2/strong/text()").get()
        if rent:
            if "pw" in rent:
                price = rent.split(".")[0].split("£")[1].strip()
                item_loader.add_value("rent", str(int(price)*4))
            else:
                price = rent.split("pcm")[0].split("£")[1].strip()
                item_loader.add_value("rent", price)
                
        item_loader.add_value("currency", "GBP")
        
        city = response.xpath("//meta[contains(@property,'locality')]/@content").get()
        zipcode = response.xpath("//meta[contains(@property,'postal')]/@content").get()
        if city:
            item_loader.add_value("address", city)
            item_loader.add_value("city", city)
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        room_count = response.xpath("//h2/text()[contains(.,'Bedroom')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[1].strip())

        bathroom_count = response.xpath("//ul/li[contains(.,'bathrooms')]/text()[not(contains(.,'bedroom'))]").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(" ")[0]
            if bathroom_count.isdigit():
                item_loader.add_value("bathroom_count", bathroom_count)
            else:
                try:
                    item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count))
                except: pass
        
        external_id = response.xpath("//p//text()[contains(.,'Ref')]").get()
        if external_id:
            external_id = external_id.split("Ref:")[1].split(",")[0].strip()
            item_loader.add_value("external_id", external_id)
        
        available_date = response.xpath("//p//text()[contains(.,'available')]").get()
        if available_date:
            available_date = available_date.split("available:")[1].strip().replace(",","")
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        latitude = response.xpath("//meta[contains(@property,'latitude')]/@content").get()
        longitude = response.xpath("//meta[contains(@property,'longitude')]/@content").get()
        if latitude or longitude:
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        desc = " ".join(response.xpath("//div[contains(@class,'twofif')]//p/text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc))
        else:
            desc = " ".join(response.xpath("//div/h1[contains(.,'inclusive')]//following-sibling::*/text()").getall())
            if desc:
                item_loader.add_value("description",desc.strip())
        images = [x for x in response.xpath("//div[contains(@class,'grid_gallery')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        energy_label = response.xpath("//div[contains(@class,'twofif')]//p/text()[contains(.,'EPC')]").get()
        if energy_label:
            energy_label = energy_label.split(" ")[-1]  
            item_loader.add_value("energy_label", energy_label)
        furnished = response.xpath("//div/ul/li[contains(.,'Furnished') or contains(.,'furnished')]//text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        parking = response.xpath("//div/ul/li[contains(.,' parking') or contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
            
        if "Terrace" in title:
            item_loader.add_value("terrace", True)
        
        item_loader.add_value("landlord_name", "MY HOUSE LETTINGS & PROPERTY MANAGEMENT")
        item_loader.add_value("landlord_phone", "0191 265 7000")
        item_loader.add_value("landlord_email","info@myhouse-ne.co.uk")
        
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
