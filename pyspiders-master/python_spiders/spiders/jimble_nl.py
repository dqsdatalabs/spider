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
    name = 'jimble_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'en'
    start_urls = ["https://jimble.nl/view-properties/"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='expat-property-container ']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Jimble_PySpider_netherlands")
        item_loader.add_value("external_link", response.url)

        f_text = "".join(response.xpath("//div[contains(@class,'wpb_text_column wpb_content_element ')]/div//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return

        item_loader.add_css("title","title")
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        
        rent = response.xpath("//span[contains(.,'Price')]//following-sibling::span[1]/text()[contains(.,'€')]").get()
        price = ""
        if rent:
            price = rent.split("per")[0].split("€")[1].strip()
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        
        available_date = response.xpath("//span[contains(.,'Available')]//following-sibling::span[1]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.replace(",",""), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
                
        square_meters = response.xpath("//span[contains(.,'Surface')]//following-sibling::span[1]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        room_count = response.xpath("//span[contains(.,'Bedroom')]//following-sibling::span[1]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
            
        bathroom_count = response.xpath("//span[contains(.,'Bathroom')]//following-sibling::span[1]/text()").get()
        if bathroom_count:
            if "." in bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.split(".")[0])
            else:
                item_loader.add_value("bathroom_count", bathroom_count)
        
        desc = " ".join(response.xpath("//div[contains(@class,'vc_col-sm-8')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[contains(@class,'cell')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        furnished = response.xpath("//span[contains(.,'Furnished')]//following-sibling::span[1]/text()").get()
        if furnished:
            if "Yes" in furnished:
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
        
        deposit = response.xpath("//ul/li[contains(.,'Deposit')]//text()").get()
        if deposit:
            deposit = deposit.split(":")[1].strip().split(" ")[0]
            deposit = int(deposit)*int(price)
            item_loader.add_value("deposit", deposit)
        else:
            deposit = response.xpath("//ul/li[contains(.,'deposit')]//text() | //div/p//text()[contains(.,'deposit')]").get()
            deposit2= ""
            if deposit:
                deposit2 = deposit.strip().split(" ")[0]
                if deposit2.isdigit():
                    deposit2 = int(deposit2)*int(price)
                else:
                    try:
                        deposit2 = w2n.word_to_num(deposit2)*int(price)
                    except:
                        deposit2 = deposit.strip().split(" ")[1]
                        if deposit2.isdigit():
                            deposit2 = int(deposit2)*int(price)
                if deposit2:
                    item_loader.add_value("deposit", deposit2)
                           
        
        if "for the utilities" in desc:
            utilities = desc.split("for the utilities")[0].strip().split(" ")[-1].replace("\u20ac","").replace(",-","")
            item_loader.add_value("utilities", utilities)
            
        
        elevator = response.xpath("//ul/li[contains(.,'Elevator')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        pets_allowed = response.xpath("//ul/li[contains(.,'Pets')]//text()").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)
        
        balcony = response.xpath("//ul/li[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//ul/li[contains(.,'terrace') or contains(.,'Terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        item_loader.add_xpath("latitude", "//div/@data-markerlat")
        item_loader.add_xpath("longitude", "//div/@data-markerlon")
        
        item_loader.add_value("landlord_name", "JIMBLE")
        item_loader.add_value("landlord_phone", "31(0)20 846 6002")
        item_loader.add_value("landlord_email", "info@jimble.nl")
        
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