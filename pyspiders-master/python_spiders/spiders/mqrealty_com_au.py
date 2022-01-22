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
    name = 'mqrealty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_url = "http://mqrealty.com.au/rentals/all-properties"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'propertyListItem')]"):
            follow_url = response.urljoin(item.xpath(".//div[@class='btnPos']/a/@href").get())
            property_type = item.xpath("./@class").get()
            if property_type:
                if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type":get_p_type_string(property_type)})

        next_button = response.xpath("//a[contains(.,'NEXT')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id",response.url.split("/")[-1])
        item_loader.add_value("external_source", "Mqrealty_Com_PySpider_australia")

        title = response.xpath("//title//text()").get()
        item_loader.add_value("title", title)

        deposit_taken = response.xpath("//h2/span[@class='textBold textUppercase']/text()[contains(.,'Deposit')]").extract_first()
        if deposit_taken:return
        
        rent = response.xpath("//span[contains(.,'$') or contains(.,'/w') or contains(.,'/W') or contains(.,'eek')]//text()").get()
        if rent:
            try:
                price = rent.split("$")[1].strip().split(" ")[0].replace("/W","").replace("/w","")
            except:
                price = rent.split("/W")[0].strip().replace("/w","").split(",")[0].replace("$","").split(" ")[0]
            
            if "-" in price:
                price = price.split("-")[0]
            item_loader.add_value("rent", int(price)*4)

        item_loader.add_value("currency", "AUD")

        city = response.xpath("//div[contains(@class,'propertyAddress')]//span//text()").get()
        if city:
            item_loader.add_value("city", city)
        
        address = "".join(response.xpath("//div[contains(@class,'propertyAddress')]//span//text()").getall())
        if address:
            item_loader.add_value("address", address)

        desc = " ".join(response.xpath("//div[contains(@class,'propertyDescription')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        if "floor " in desc:
            floor = desc.split("floor ")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor)

        room_count = response.xpath("//li[contains(.,'Bed')]//span//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif "studio" in desc.lower():
            item_loader.add_value("room_count", "1")
            
        bathroom_count = response.xpath("//li[contains(.,'Bath')]//span//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//span[contains(.,'From')]/parent::p//text()").getall())
        if available_date:
            available_date= re.sub('\s{2,}', ' ', available_date.strip())
            available_date = available_date.split(":")[1].strip()
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//i[contains(@class,'check')]/parent::span/parent::li[contains(.,'Parking')]/b/text() | //b[contains(.,'Carport')]/following-sibling::span/text()[.!='0']").get()
        garage = response.xpath("//i[contains(@class,'check')]/parent::span/parent::li[contains(.,'Garage')]/b/text()").get()
        if parking or garage:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//li//b[contains(.,'Car Space')]//text()").get()
            if parking:
                item_loader.add_value("parking", True)

        images = [x for x in response.xpath("//li//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        

        item_loader.add_xpath("latitude", "substring-after(substring-before(//style[contains(.,'road')],','),'C-')")
        item_loader.add_xpath("longitude", "substring-after(substring-before(//style[contains(.,'road')],'&key'),',')")

        name = response.xpath("//h2[contains(@class,'staffName')]/text()").get()
        surname = response.xpath("//h2[contains(@class,'staffName')]/span/text()").get()
        item_loader.add_value("landlord_name", name + " " + surname)
        landlord_phone = response.xpath("//a[contains(@class,'staffName')]//text()").get()
        item_loader.add_value("landlord_phone", landlord_phone)
        
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None