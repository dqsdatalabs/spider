# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from word2number import w2n

class MySpider(Spider):
    name = 'crabbcurtis_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        yield Request("https://www.crabbcurtis.co.uk/rental-property/", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[contains(@class,'property_list_item')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = " ".join(response.xpath("//div[@id='propertydescription']//text()").getall()).strip()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return
        
        item_loader.add_value("external_source", "Crabbcurtis_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("id=")[1])
        
        title = response.xpath("normalize-space(//div[@id='property_details']/h1/text())").get()
        if title:
            item_loader.add_value("title", title)
            item_loader.add_value("address", title)
            item_loader.add_value("city", title.split(",")[-1])
        
        rent = response.xpath("//h2[@class='price']/text()").get()
        if rent:
            rent = rent.split(" ")[0].replace("£","").replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        description = " ".join(response.xpath("//div[@id='propertydescription']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [x for x in response.xpath("//div[contains(@class,'et_pb_gallery_item')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
            
        room_count = response.xpath("//li[contains(.,'Bedroom')]/text()").get()
        if room_count:
            room_count = room_count.split(" ")[0]
            try:
                item_loader.add_value("room_count", w2n.word_to_num(room_count))
            except:
                if "bedroom" in description:
                    room_count = description.split("bedroom")[0].replace("double","").strip().split(" ")[-1]
                    try:
                        item_loader.add_value("room_count", w2n.word_to_num(room_count))
                    except: pass
        
        terrace = response.xpath("//li[contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        balcony = response.xpath("//li[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'Garage')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,' furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        import dateparser
        available_date = response.xpath("//li[contains(.,'Available')]/text()").get()
        if available_date and "Immediately" not in available_date:
            available_date = available_date.split("Available")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        floor = response.xpath("//li[contains(.,'Floor')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(" ")[0])
        
        energy_label = response.xpath("//li[contains(.,'EPC')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(":")[1].strip())

        item_loader.add_value("landlord_name", "Crabb Curtis ")
        item_loader.add_value("landlord_phone", "01926 88 88 44")
        item_loader.add_value("landlord_email", "leamington@crabbcurtis.co.uk")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None