# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re
from python_spiders.helper import ItemClear

class MySpider(Spider):
    name = 'mullanproperty_com'
    execution_type='testing'
    country='ireland'
    locale='en'
    def start_requests(self):
        start_urls = [
            "http://www.mullanproperty.com/properties/viewlist.asp?cat=0&LocationID=2",
            "http://www.mullanproperty.com/properties/viewlist.asp?cat=0&LocationID=9",
            "http://www.mullanproperty.com/properties/viewlist.asp?cat=0&LocationID=7",
            "http://www.mullanproperty.com/properties/viewlist.asp?cat=0&LocationID=18",
        ]
        for start_url in start_urls: yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[contains(.,'More Details')]/.."):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            property_type = "".join(item.xpath("./strong//text()").getall())
            if property_type:
                if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("ID=")[1])
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Mullanproperty_PySpider_ireland", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="normalize-space(//div/strong/text())", input_type="F_XPATH")
        address = response.xpath("normalize-space(//div/strong/text())").get()
        if address:
            address = address.split("-")[-1].strip()
            item_loader.add_value("address",address)
            item_loader.add_value("city",address.split(",")[-2].strip())
            item_loader.add_value("zipcode",address.split(",")[-1].strip())
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="substring-before(substring-after(//table[contains(@bordercolor,'#C2CEDA')]//td[contains(.,'Bedroom')]//text(), '•'), 'Bed')", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="substring-before(substring-after(//table[contains(@bordercolor,'#C2CEDA')]//td[contains(.,'Bathroom')]//text(), '•'), 'Bath')", input_type="F_XPATH", get_num=True)
        
        rent_week = response.xpath("//ul/li[contains(.,'per week')]/font/text()").get()
        rent = response.xpath("//div[@align='center']//text()[contains(.,'/ week')]").get()
        rent1 = response.xpath(" //div[@align='center']//text()[contains(.,'per apartment per week')]").get()       
        if rent_week:
            rent_week = rent_week.split("£")[1].strip().split(" ")[0]
            item_loader.add_value("rent", int(rent_week)*4)
        elif rent1:
            rent1 = rent1.split("/")[1].split("£")[1].strip().split(" ")[0]
            item_loader.add_value("rent", int(rent1)*4)
        elif rent:
            rent = rent.split("£")[1].strip().split("/")[0]
            item_loader.add_value("rent", int(rent)*4)
        else:
            rent = response.xpath("//div[@align='center']//text()[contains(.,'£')]").get()
            if rent:
                rent = rent.split("/ week")[0].split("£")[1].strip().split(".")[0]
                item_loader.add_value("rent", rent)
        
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        
        desc = " ".join(response.xpath("//table[@cellpadding='5']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "deposit" in desc:
            deposit = desc.split("deposit")[0].split("\u00a3")[1]
            item_loader.add_value("deposit", deposit)

        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1].replace("th","").replace("(","")
            if floor.isdigit():
                item_loader.add_value("floor", floor)
        
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'furnished')]//text()", input_type="M_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//table[contains(@bordercolor,'#C2CEDA')]//td[contains(.,'Balcony')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//table[contains(@bordercolor,'#C2CEDA')]//td[contains(.,'Parking')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//li[contains(.,'Washing')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//@src[contains(.,'jpg') and contains(.,'http://www.')]", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//a/@href[contains(.,'maps')]", input_type="F_XPATH", split_list={"sll=":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//a/@href[contains(.,'maps')]", input_type="F_XPATH", split_list={"sll=":1, ",":1,"&":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="MULLAN PROPERTY", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="44 (0)28 9032 2228", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@mullanproperty.com", input_type="VALUE")

        # print("----------", [x for x in response.xpath("").getall()])
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    else:
        return None