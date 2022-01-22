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

class MySpider(Spider):
    name = 'cityresidentialbourkestreet_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.cityresidentialbourkestreet.com.au/search-results/?list=lease&property_type%5B%5D=Apartment&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=", "property_type": "apartment"},
	        {"url": "https://www.cityresidentialbourkestreet.com.au/search-results/?property_type%5B%5D=Studio&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=#apsp", "property_type": "studio"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 1)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'listing-item')]/div/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 1 or seen:
            if response.meta.get('property_type') == "apartment":
                url = f"https://www.cityresidentialbourkestreet.com.au/search-results/page/{page}/?list=lease&property_type%5B0%5D=Apartment&min_price&max_price&bedrooms&bathrooms&carspaces"
            else:
                url = f"https://www.cityresidentialbourkestreet.com.au/search-results/page/{page}/?list=lease&property_type%5B0%5D=Studio&min_price&max_price&bedrooms&bathrooms&carspaces"
                
            yield Request(url, callback=self.parse, meta={"property_type": response.meta.get('property_type'), "page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Cityresidentialbourkestreet_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h4[contains(@class,'address')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h4[contains(@class,'address')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h4[contains(@class,'address')]/text()", input_type="F_XPATH", split_list={" ":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='price']/text()", input_type="F_XPATH", get_num=True, per_week=True, split_list={"$":1, " ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/i[contains(@class,'bed')]/parent::li/span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li/i[contains(@class,'bath')]/parent::li/span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li/i[contains(@class,'car')]/parent::li/span/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//p[contains(@class,'-id')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//label[contains(.,'Available')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//label[contains(.,'Bond')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={"$":1}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//li[contains(.,'Pool')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished')]/text()", input_type="F_XPATH", tf_item=True)
        
        desc = " ".join(response.xpath("//div[@class='detail-description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "sqm" in desc:
            square_meters = desc.split("sqm")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", square_meters.replace("(",""))
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1].replace("th","")
            if floor.isdigit():
                item_loader.add_value("floor", floor)
            
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='carousel']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'L.marker')]/text()", input_type="F_XPATH", split_list={"L.marker([":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'L.marker')]/text()", input_type="F_XPATH", split_list={"L.marker([":1,",":1,"]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[contains(@class,'agent-detail')]//strong//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[contains(@class,'agent-detail')]//a/text()[contains(.,'@')]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//div[contains(@class,'agent-detail')]//a[contains(@href,'tel')]/text()", input_type="F_XPATH")

        yield item_loader.load_item()