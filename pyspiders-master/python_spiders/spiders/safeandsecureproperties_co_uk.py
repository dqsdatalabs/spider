# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'safeandsecureproperties_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://safeandsecureproperties.co.uk/property-search/?department=residential-lettings&minimum_price=&maximum_price=&price_range=&minimum_rent=&maximum_rent=&property_type=26&location=&address_keyword=&bedrooms=&minimum_bedrooms=&minimum_bathrooms=&furnished=&parking=&outside_space=&tenure=",
                ],
                "property_type" : "apartment",
                "type" : "26",
            },
            {
                "url" : [
                    "https://safeandsecureproperties.co.uk/property-search/?department=residential-lettings&minimum_price=&maximum_price=&price_range=&minimum_rent=&maximum_rent=&property_type=13&location=&address_keyword=&bedrooms=&minimum_bedrooms=&minimum_bathrooms=&furnished=&parking=&outside_space=&tenure=",
                    
                ],
                "property_type" : "house",
                "type" : "13",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "type":url["type"]})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='thumbnail']/a"):
            status = item.xpath(".//div[contains(@class,'flag')]/text()").get()
            if status and ("under" in status.lower() or "agreed" in status.lower()):
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            base_type = response.meta["type"]
            p_url = f"https://safeandsecureproperties.co.uk/property-search/page/{page}/?department=residential-lettings&minimum_price&maximum_price&price_range&minimum_rent&maximum_rent&property_type={base_type}&location&address_keyword&bedrooms&minimum_bedrooms&minimum_bathrooms&furnished&parking&outside_space&tenure"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "type":base_type})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Safeandsecureproperties_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//li[contains(.,'Ref:')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1[contains(@class,'title')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1[contains(@class,'title')]/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1[contains(@class,'title')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'Bedrooms:')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(.,'Bathrooms:')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[@class='features']//li[contains(.,'Terrace')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="pets_allowed", input_value="//div[@class='features']//li[contains(.,'Pets Considered')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking') or contains(.,'Garage')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='description']//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@class='summary']//text()[contains(.,'BOND') or contains(.,'Bond')]", input_type="M_XPATH", split_list={"£":-1, "*":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@class='slides']//img/@data-lazy-src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li[contains(.,'Available') and not(contains(.,'Availability'))]/text()", input_type="F_XPATH", replace_list={"Available":"", "!":""})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//a[contains(@href,'maps')]/@href", input_type="F_XPATH", split_list={"/@":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//a[contains(@href,'maps')]/@href", input_type="F_XPATH", split_list={"/@":1, ",":1, ",":0})
        
        rent = response.xpath("//div[@class='price']//text()").get()
        if rent:
            if "pw" in rent:
                rent = rent.split("pw")[0].split("£")[1].strip()
                item_loader.add_value("rent", int(rent)*4)
            elif "pcm" in rent:
                rent = rent.split("pcm")[0].split("£")[1].strip()
                item_loader.add_value("rent", rent)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        
        floor = response.xpath("//div[@class='description']//p//text()[contains(.,'floor')]").get()
        if floor:
            floor = floor.split("floor")[0].strip().split(" ")[-1]
            if "lami" not in floor and "each" not in floor:
                item_loader.add_value("floor", floor.capitalize())
         
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="SAFE & SECURE PROPERTIES", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0191 385 4477", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@safeandsecureproperties.co.uk", input_type="VALUE")
        
        yield item_loader.load_item()