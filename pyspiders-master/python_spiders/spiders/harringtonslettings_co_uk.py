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
from word2number import w2n
import re

class MySpider(Spider):
    name = 'harringtonslettings_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://harringtonslettings.co.uk/search/?department=residential-lettings"]

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='b-property-card__img-link']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Harringtonslettings_Co_PySpider_"+ self.country)

        properties = " ".join(response.xpath("//h3[contains(.,'Property Features')]/following-sibling::ul/li/text()").getall())
        if get_p_type_string(properties):
            item_loader.add_value("property_type", get_p_type_string(properties))
        else:
            summary = "".join(response.xpath("//h3[contains(.,'Property Summary')]/following-sibling::p/text()").getall())
            if get_p_type_string(summary):
                item_loader.add_value("property_type", get_p_type_string(summary))
            else:
                return
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = response.xpath("//h1/text()").get()
        if address:
            
            item_loader.add_value("address", address)
            
            zipcode = address.split(",")[-1].strip()
            status = False
            if not zipcode.isalpha():
                for i in zipcode:
                    if i.isdigit():
                        status = True
                # if status:
                    # item_loader.add_value("zipcode", zipcode)
            if "," in address :
                if zipcode:
                    city = address.split(zipcode)[0].strip().strip(",")
                    if "," in city:
                        item_loader.add_value("city", city.split(",")[-1])
                    else:
                        item_loader.add_value("city", city)
                else:
                    item_loader.add_value("city", address.split(",")[-1].strip())
            else:
                item_loader.add_value("city", address.strip())
        
        zipcode = response.xpath("//p[@class='b-property-header__address']//text()[normalize-space()]").get()
        if zipcode:
            zipcode =  zipcode.strip().split(",")[-1].strip()
            if not zipcode.replace(" ","").isalpha():
                item_loader.add_value("zipcode", zipcode)
                    
        rent = response.xpath("//p[contains(@class,'price')]/span/text()").get() 
        if rent:
            price = rent.split(" ")[0].split("£")[1].replace(",","").split(".")[0]
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")
        
        available_date = "".join(response.xpath("//p[contains(@class,'available')]//text()[normalize-space()]").getall())
        if available_date:
            if "Now" in available_date:
                available_date = datetime.now()
                date2 = available_date.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
                
            else:
                available_date = available_date.split(":")[1].strip()
                date_parsed = dateparser.parse(
                        available_date, date_formats=["%d/%m/%Y"]
                    )
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        desc = " ".join(response.xpath("//div/h3[contains(.,'Summary')]/../p//text()").getall())
        if desc:
            desc = desc.replace("\r","").replace("\n","")
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        deposit = " ".join(response.xpath("substring-before(//div/h3[contains(.,'Summary')]/../p//text()[contains(.,'DEPOSIT') and contains(.,'£')],'.')").getall())
        if deposit:
            item_loader.add_value("deposit", deposit.replace(",","").strip())    
        room_count = response.xpath("//div/h3[contains(.,'Features')]/../ul/li[contains(.,'Bedroom')]/text()").get()
        if room_count:
            room_count = room_count.replace("Double","").split("Bedroom")[0].strip()
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
            elif " " in room_count:
                room_count = room_count.split(" ")[0]
                try:
                    item_loader.add_value("room_count", w2n.word_to_num(room_count))
                except: pass
            else:
                try:
                    item_loader.add_value("room_count", w2n.word_to_num(room_count))
                except: pass
        elif "bedroom" in desc.lower():
            room = desc.lower().split("bedroom")[0].replace("double","").strip().split(" ")[-1]
            if room == "a" or room == "single":
                room = desc.lower().split("bedroom")[1].replace("double","").strip().split(" ")[-1]
                try:
                    item_loader.add_value("room_count", w2n.word_to_num(room))
                except:
                    item_loader.add_value("room_count", "1")
            else:
                try:
                    item_loader.add_value("room_count", w2n.word_to_num(room))
                except:
                    pass
                
        bathroom_count = response.xpath("//div/h3[contains(.,'Features')]/../ul/li[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(" ")[0]
            item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count))

        floor_plan_images = response.xpath("//a[contains(@data-fancybox,'floor')]/img/@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        furnished = response.xpath("//div/h3[contains(.,'Features')]/../ul/li[contains(.,'Furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//div/h3[contains(.,'Features')]/../ul/li[contains(.,'Lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
            

        if "EPC RATING:" in desc:
            energy = desc.split("EPC RATING:")[1].strip().split(" ")[0]
            item_loader.add_value("energy_label", energy[0])

            
        images = [ x for x in response.xpath("//div[@id='lightgallery']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude = response.xpath("//div/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        
        longitude = response.xpath("//div/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)

        item_loader.add_value("landlord_name", "Harringtons Lettings")

        phone = "".join(response.xpath("//div[contains(@class,'tel')]/a/text()").getall())
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())

        item_loader.add_value("landlord_email", "viewings@harringtonslettings.co.uk")
        
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
