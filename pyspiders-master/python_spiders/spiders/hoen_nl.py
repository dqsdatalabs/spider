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
class MySpider(Spider):
    name = 'hoen_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source = 'Hoen_PySpider_netherlands'
    start_urls = ["https://www.hoen.nl/huurwoningen"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='field-status']"):
            follow_url = response.urljoin(item.xpath("./../@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        f_text = "".join(response.xpath("//div[contains(@class,'field--name-field-details')]/p/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        item_loader.add_value("external_source", "Hoen_PySpider_netherlands")
        item_loader.add_xpath("title", "//title/text()")        
        external_id = response.xpath("//link[@rel='shortlink']//@href").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split('node/')[-1])
        address =", ".join(response.xpath("//div[contains(@class,'object-side-info')]//div[@class='block-content']//text()[normalize-space()]").extract()) 
        if address:
            item_loader.add_value("address",address.strip() ) 

        item_loader.add_xpath("zipcode", "//div[contains(@class,'field--name-field-address-postcode')]//text()")
        item_loader.add_xpath("city", "//div[contains(@class,'field--name-field-address-town')]//text()")
        item_loader.add_xpath("room_count", "//li/label[contains(.,'slaapkamer')]/following-sibling::div//text()")
         
        rent ="".join(response.xpath("//div/h3[div[contains(@class,'field--name-field-price')]]//text()").extract())
        if rent:     
            item_loader.add_value("rent_string", rent.strip())  
        utilities ="".join(response.xpath("//div[contains(@class,'field--name-field-details')]//text()[contains(.,'servicekosten') and contains(.,'€')]").extract())
        if utilities:     
            utilities = utilities.split("€")[1].strip()
            item_loader.add_value("utilities", utilities) 

        heating_cost ="".join(response.xpath("//div[contains(@class,'field--name-field-details')]//text()[contains(.,'stookkosten') and contains(.,'€')]").extract())
        if heating_cost:     
            heating_cost = heating_cost.split("€")[1].strip()
            item_loader.add_value("heating_cost", heating_cost) 

        available_date = response.xpath("//li/label[contains(.,'Beschikbaar')]/following-sibling::p//text()").extract_first() 
        if available_date:
            date_parsed = dateparser.parse(available_date.replace("per direct","now").strip())
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
    
        square =response.xpath("//li/label[contains(.,'Oppervlakte')]/following-sibling::div//text()").extract_first()
        if square:
            item_loader.add_value("square_meters", square) 

        desc = " ".join(response.xpath("//div[contains(@class,'field--name-field-details')]//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "waarborgsom bedraagt minimaal" in desc:
                deposit = desc.split("waarborgsom bedraagt minimaal")[1].split(" maand")[0].strip()
                if deposit.isdigit():
                    if rent:
                        price = rent.split("€")[1].split(",")[0].replace(".","").strip()
                        deposit = int(deposit)*int(price)
                    item_loader.add_value("deposit", deposit) 
              
        images = [response.urljoin(x) for x in response.xpath("//div[@id='object-carousel-modal']//div/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Hoen Vastgoedbeheer")
        item_loader.add_value("landlord_phone", "020-305 44 77")
        item_loader.add_value("landlord_email", "verhuur@hoen.nl")
              
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None