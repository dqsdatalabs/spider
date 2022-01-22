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
    name = 'coastlineagency_com'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_url = "https://www.coastlineagency.com/renting/homes-to-rent/"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='listing column']//section/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            property_type = item.xpath("./p[@class='property_type']/text()").get()
            if property_type:
                if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type":get_p_type_string(property_type)})
            
        next_button = response.xpath("//a[@class='next_page_link']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Coastlineagency_PySpider_australia")
        
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        
        address = " ".join(response.xpath("//div[@class='address-wrap']//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
        zipcode = response.xpath("//script[contains(.,'oneformbtn.papf_proppc')]/text()").get()
        if zipcode:
            zipcode1= zipcode.split(".papf_proppc = '")[-1].split("'")[0]
            zipcode2= zipcode.split(".papf_propstat = '")[-1].split("'")[0]
            item_loader.add_value("zipcode", zipcode2+" "+zipcode1)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='suburb']//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(@class,'bedroom')]//text()[not(contains(.,'0'))]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(@class,'bathroom')]//text()[not(contains(.,'0'))]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[@class='price']/text()", input_type="F_XPATH", get_num=True, split_list={".":0}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        
        desc = " ".join(response.xpath("//div[@id='property-description']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "sqm" in desc:
            square_meters = desc.split("sqm")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", square_meters)
        
        if "floor " in desc:
            floor = desc.split("floor ")[0].strip().split(" ")[-1]
            if "out" not in floor:
                item_loader.add_value("floor", floor)
        
        images = [x.split("url(")[1].split(")")[0] for x in response.xpath("//figure//@style[contains(.,'background')]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("external_id", response.url.split("/")[-2])
        
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'L.marker')]/text()", input_type="F_XPATH", split_list={"L.marker([":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'L.marker')]/text()", input_type="F_XPATH", split_list={"L.marker([":1, ",":1, "]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[@id='property-description']//p//text()[contains(.,'balcon')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[@id='property-description']//p//text()[contains(.,'terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@id='property-description']//p//text()[contains(.,' furnished')]", input_type="F_XPATH", tf_item=True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(@class,'car')]//text()[not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//p[@class='name']//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//p[@class='contact']//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="letting@coastlineagency.com", input_type="VALUE")
        
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