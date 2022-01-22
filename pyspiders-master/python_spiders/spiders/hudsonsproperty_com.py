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
    name = 'hudsonsproperty_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'  
    start_urls = ["https://www.hudsonsproperty.com/property-lettings/property-to-rent-in-west-end-and-central-london?"]

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@id='resultsbox']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Hudsonsproperty_PySpider_"+ self.country)
        desc = "".join(response.xpath("//p[@class='mb-5']/text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: 
            return
        externalid=response.url
        if externalid:
            item_loader.add_value("external_id",externalid.split("/")[-1])
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))

        address = response.xpath("//h1[@class='address-text']/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(",")[-1].strip()
            
            city = address.split(zipcode)[0].strip().strip(",").strip().strip(",")
            if "," in city:
                city = city.split(",")[-1].strip()
                item_loader.add_value("city", city)
            else:
                item_loader.add_value("city", city)
                
            item_loader.add_value("zipcode", zipcode)
        
        rent = "".join(response.xpath("//div[contains(@class,'price')]/text()").getall())
        if rent:
            price = rent.strip().split("pcm")[0].split("(£")[1].replace(",","").strip()
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")
        

        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))


        
        if "sq ft" in desc:
            square_meters = desc.split("sq ft")[0].strip().split(" ")[-1]
            sqm = str(int(int(square_meters.replace(",",""))* 0.09290304))
            item_loader.add_value("square_meters", sqm)

            
        room_count = response.xpath("//div[@class='bedroom']/span[2]/text()").get()
        room = response.xpath("//ul/li[contains(.,'studio')]/text()").get()
        if room_count:
            if room_count != "0":
                item_loader.add_value("room_count", room_count)
            elif room:
                item_loader.add_value("room_count", "1")
            elif "studio" in desc.lower():
                item_loader.add_value("room_count", "1")
        
        bathroom_count = response.xpath("//div[@class='bathroom']/span[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        floor = response.xpath("//ul/li[contains(.,'floor') or contains(.,'Floor')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.lower().split("floor")[0].strip())
        
        available_date = response.xpath("//ul/li[contains(.,'Available')]/text()").get()
        if available_date:
            if "now" in available_date.lower():
                available_date = datetime.now()
                date2 = available_date.strftime("%Y-%m-%d")
                item_loader.add_value("available_date",date2)
        
        images = [ x for x in response.xpath("//div[@class='sp-slides']//div/img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        parking = response.xpath("//div[@class='parking']/span[2]/text()").get()
        if parking:
            if "NO" in parking:
                item_loader.add_value("parking", False)
            elif "YES" in parking:
                item_loader.add_value("parking", True)
        
        furnished = response.xpath("//ul/li[contains(.,'Furnished')]/text()").get()
        unfurnished = response.xpath("//ul/li[contains(.,'Unfurnished')]/text()").get()
        if unfurnished:
            item_loader.add_value("furnished", False)
        elif furnished:
            item_loader.add_value("furnished", True)
            
        elevator = response.xpath("//ul/li[contains(.,'Lift') or contains(.,'lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
            
        balcony = response.xpath("//ul/li[contains(.,'balcon') or contains(.,'Balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
            
        swimming_pool = response.xpath("//ul/li[contains(.,'pool') or contains(.,'Pool')]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        washing_machine = response.xpath("//ul/li[contains(.,'washing machine') or contains(.,'Washing machine')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
            
        item_loader.add_value("landlord_name", "HUDSONS")
        item_loader.add_value("landlord_phone", "020 7323 2277")
        item_loader.add_value("landlord_email", "info@hudsonsproperty.com")

        if not item_loader.get_collected_values("furnished"):
            features = " ".join(response.xpath("//ul[@class='mb-0']/li/text()").getall())
            if features and "furnish" in features:
                item_loader.add_value("furnished", True) 

            if features and "available" in features.lower() and not item_loader.get_collected_values("available_date"):
                available_date = features.lower().split("available")[1].strip().split(" ")[0].strip()
                if available_date and "now" in available_date:
                    item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
                elif available_date:
                    date_parsed = dateparser.parse(available_date)
                    if date_parsed:
                        date3 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date3)
        
        latlng = response.xpath("//div[@id='google_map']/iframe/@src").get()
        if latlng:
            lat = latlng.split("=")[-1].split(",")[0].strip()
            lng = latlng.split("=")[-1].split(",")[1].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)

            
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    else:
        return None
