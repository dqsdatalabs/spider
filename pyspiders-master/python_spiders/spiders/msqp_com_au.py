# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import scrapy
from python_spiders.helper import ItemClear
import re
class MySpider(Spider):
    name = 'msqp_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    headers = {
        'authority': 'www.msqp.com.au',
        'accept': '*/*',
        'x-requested-with': 'XMLHttpRequest',
        'referer': 'https://www.msqp.com.au/lease',
        'accept-language': 'tr,en;q=0.9',
    }

    def start_requests(self):

        start_url = "https://www.msqp.com.au/data/results/?listing_sale_method=Lease&deletecache=0&gallery=1&pg=1"
        yield Request(start_url, headers=self.headers, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        data = json.loads(response.body)
        selector = scrapy.Selector(text=data, type="html")
        for item in selector.xpath("//li[@class='propertyListing']/div/a[1]/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item)

        if page == 2 or seen: 
            f_url = f"https://www.msqp.com.au/data/results/?listing_sale_method=Lease&deletecache=0&gallery=1&pg={page}"
            yield Request(f_url, headers=self.headers, callback=self.parse, meta={"page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_type = response.xpath("//aside[@class='side-content']/h3/text()").get()
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id",response.url.split("estate/")[-1].split("/")[0])


        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Msqp_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//span/h1//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span/h1//text()[2]", input_type="F_XPATH")
        item_loader.add_value("zipcode",response.url.split("vic-")[-1])
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//aside/h3/../div[1]/text()", input_type="F_XPATH", get_num=True,per_week=True, split_list={"$":1," ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span/i[contains(@class,'bed')]/parent::span/em/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span/i[contains(@class,'bath')]/parent::span/em/text()", input_type="F_XPATH", get_num=True)
        
        desc = " ".join(response.xpath("//article//p/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "sqm" in desc:
            square_meters = desc.split("sqm")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", square_meters.replace("(",""))
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1].replace("th","").replace("st","")
            if floor.isdigit():
                item_loader.add_value("floor", floor) 
                
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li[contains(.,'Available')]//text()", input_type="F_XPATH", split_list={"Available":1})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li[contains(.,'Bond')]//text()", input_type="F_XPATH", split_list={"$":1}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//li[contains(.,'Dishwasher')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//li[contains(.,'Pool')]/text()", input_type="M_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='slick-slider']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'latLng')]/text()", input_type="F_XPATH", split_list={"lat:":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'latLng')]/text()", input_type="F_XPATH", split_list={"lng:":1,"}":0})
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//li[@class='staff-member']//a[contains(@class,'name')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//li[@class='staff-member']//a[contains(@class,'phone')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="hello@msqp.com.au", input_type="VALUE")

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