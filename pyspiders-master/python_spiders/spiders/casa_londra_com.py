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
    name = 'casa_londra_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.casa-londra.com/en/long-lettings"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='summary-read-more-link']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Casa_Londra_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        f_text = " ".join(response.xpath("//div[@class='sqs-block-content']/p//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//div[@class='sqs-block-content']/h1//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)
        propert_type = response.xpath("//div/h2[contains(.,'Details')]/parent::div/p//text()[contains(.,'Property')]").get()
        if propert_type and "studio" in propert_type:
            item_loader.add_value("property_type", "studio")
        elif prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = response.xpath("//h1/a/text()").get()
        if address:
            item_loader.add_value("address", address)         
            city = address.split(",")[-1].strip().split(" ")[0]      
            if not city.isalpha():   
                if "," in address:  
                    item_loader.add_value("zipcode", city)
                    item_loader.add_value("city", address.split(",")[-2].strip().replace(city,"")) 
                else:
                    item_loader.add_value("zipcode", address.split(" ")[-1].strip())
            else:
                zipcode = address.split(city)[1].strip()
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)
        
        square_meters = response.xpath("//div/h2[contains(.,'Details')]/parent::div/p//text()[contains(.,'Size')]").get()
        if square_meters:
            square_meters = square_meters.split(":")[1].strip().split(" ")[0]
            if square_meters.isdigit():
                item_loader.add_value("square_meters", square_meters)
        
        room_count = response.xpath("//div/h2[contains(.,'Details')]/parent::div/p//text()[contains(.,'Bedroom')]").get()
        if room_count:
            if "+" in room_count:
                room_count = room_count.split(":")[1].split("(")[0].strip().split("+")
                item_loader.add_value("room_count", int(room_count[0])+int(room_count[1]))
            else:
                item_loader.add_value("room_count", room_count.split(":")[1].strip())
        
        bathroom_count = response.xpath("//div/h2[contains(.,'Details')]/parent::div/p//text()[contains(.,'Bathroom')]").get()
        if bathroom_count:
            if "," in bathroom_count:
                bath = bathroom_count.split(":")[1].strip().replace(",",".")
                item_loader.add_value("bathroom_count", int(float(bath)))
            else:
                item_loader.add_value("bathroom_count", bathroom_count.split(":")[1].split("/")[0].strip())
        
        rent = response.xpath("//div/h1//text()[contains(.,'£')]").get()
        if rent:
            price = rent.split("per")[0].split("£")[1].strip().replace(",","")
            item_loader.add_value("rent", int(price)*4)
            item_loader.add_value("currency", "GBP")
        
        desc = " ".join(response.xpath("//div[contains(@id,'block-')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[@data-type='image']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude = response.xpath("//meta[@property='og:latitude']/@content").get()
        longitude = response.xpath("//meta[@property='og:longitude']/@content").get()
        if latitude or longitude:
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        if "EPC rating" in desc:
            energy_label = desc.split("EPC rating")[1].strip().split(" ")[0].replace(".","")
            item_loader.add_value("energy_label", energy_label)
        
        import dateparser
        if "available from end of" in desc:
            available_date = desc.split("available from end of")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        outdoor = response.xpath("//div/h2[contains(.,'Details')]/parent::div/p//text()[contains(.,'Outdoor')]").get()
        if outdoor:
            if "terrace" in outdoor:
                item_loader.add_value("terrace", True)
            if "balcon" in outdoor:
                item_loader.add_value("balcony", True)
            
        if "floor" in desc:
            floor = desc.split("floor")[0].replace("\xa0"," ").strip().split(" ")[-1]
            if "wood" not in floor.lower():
                item_loader.add_value("floor", floor.capitalize())
        
        
        
        name = response.xpath("//div/h2[.='Lettings ']/parent::div/p//text()").get()
        if name:
            item_loader.add_value("landlord_name", name.strip())
        
        phone = response.xpath("//div/h2[.='Lettings ']/parent::div/p//text()[contains(.,'Phone')]").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.split("+")[1].strip())
        item_loader.add_value("landlord_email", "info@casa-londra.com")
        
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