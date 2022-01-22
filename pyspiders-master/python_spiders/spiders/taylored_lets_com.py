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
import dateparser

class MySpider(Spider):
    name = 'taylored_lets_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            "https://www.taylored-lets.com/student-lets/",
            "https://www.taylored-lets.com/professional-lets/",
            "https://www.taylored-lets.com/house-shares/",
            "https://www.taylored-lets.com/family-lets/",
        ]
        for url in start_urls: yield Request(url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[contains(@class,'property-list')]/li"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'View')]/@href").get())
            agreed = item.xpath(".//span[@class='agreed' and contains(.,'Let Agreed')]").get()
            if not agreed: yield Request(follow_url, callback=self.populate_item)

        next_button = response.xpath("//li[@class='next']//a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_type = response.xpath("//div[@class='showable-details']/p[1]/text()").get()
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return
        else: return
        item_loader.add_value("external_link", response.url)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Taylored_Lets_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1//text()", input_type="F_XPATH", split_list={" in":1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1//text()", input_type="F_XPATH", split_list={" in":1})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[@class='bedrooms']/text()", input_type="M_XPATH", get_num=True, split_list={" ":0})
        
        rent = response.xpath("//li[@class='price']/text()").get()
        if rent:
            if "pw" in rent:
                rent = rent.split('£')[-1].split('p')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))           
            else:
                ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li[@class='price']/text()", input_type="F_XPATH", split_list={".":0})
            
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        #ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li[@class='date']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'parking')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'balcony')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,' furnished') or contains(.,'-furnished') or contains(.,'Furnished')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'lift')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//li[contains(.,'washing machine') or contains(.,'Washing Machine')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//li[contains(.,'dishwasher')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//div/@data-location", input_type="F_XPATH", split_list={",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//div/@data-location", input_type="F_XPATH", split_list={",":1})
        
        
        available_date = response.xpath("//li[@class='date']/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date)
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        
        desc = " ".join(response.xpath("//div[@class='showable-details']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = response.xpath("//img/@data-images[contains(.,'image')]").get()
        if images:
            images = images.split('"')
            for i in range(1,len(images)):
                if "image" in images[i]:
                    item_loader.add_value("images", images[i])
        
        if "floor " in desc:
            floor = desc.split("floor ")[0].strip().split(" ")[-1]
            if "," not in floor:
                item_loader.add_value("floor", floor)
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Taylored Lets", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0191 447 1718", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@taylored-lets.com", input_type="VALUE")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None