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
    name = 'professionalsfairfield_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://professionalsfairfield.com.au/search/?show_in_rental=true&sold=0&order_by=listing_date&order=desc&localities=&postal_code=&status=rent&suburb-box=&property_types=flat&min_price=&max_price=&min_bedrooms=&min_bathrooms=&min_carparks=&min_land_size=&land_size_units=m2",
                    "https://professionalsfairfield.com.au/search/?show_in_rental=true&sold=0&order_by=listing_date&order=desc&localities=&postal_code=&status=rent&suburb-box=&property_types=unit&min_price=&max_price=&min_bedrooms=&min_bathrooms=&min_carparks=&min_land_size=&land_size_units=m2",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://professionalsfairfield.com.au/search/?show_in_rental=true&sold=0&order_by=listing_date&order=desc&localities=&postal_code=&status=rent&suburb-box=&property_types=duplex+or+semi-detached&min_price=&max_price=&min_bedrooms=&min_bathrooms=&min_carparks=&min_land_size=&land_size_units=m2",
                    "https://professionalsfairfield.com.au/search/?show_in_rental=true&sold=0&order_by=listing_date&order=desc&localities=&postal_code=&status=rent&suburb-box=&property_types=house&min_price=&max_price=&min_bedrooms=&min_bathrooms=&min_carparks=&min_land_size=&land_size_units=m2",
                    "https://professionalsfairfield.com.au/search/?show_in_rental=true&sold=0&order_by=listing_date&order=desc&localities=&postal_code=&status=rent&suburb-box=&property_types=villa&min_price=&max_price=&min_bedrooms=&min_bathrooms=&min_carparks=&min_land_size=&land_size_units=m2",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//section[@class='results grid']/div[@class='container']/div/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//i[contains(@class,'chevron-right')]/../@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Professionalsfairfield_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='address']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[@class='address']/text()", input_type="F_XPATH", split_list={" ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='address']/text()", input_type="F_XPATH", split_list={",":-1, "NSW":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='price']/text()[contains(.,'$')]", input_type="F_XPATH", get_num=True, per_week=True, split_list={"$":1, " ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="USD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//img[@class='bed']/following-sibling::text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//img[@class='bath']/following-sibling::text()", input_type="F_XPATH", get_num=True)
        
        desc = " ".join(response.xpath("//div[@class='tab-content']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        not_list = ["timber", "carpet", "and", "tile", "wood", "float", "vinyl"]
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1]
            status = True
            for i in not_list:
                if i in floor.lower():
                    status = False
            if status:
                item_loader.add_value("floor", floor)
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='gallery-container']//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//img[@class='car']/following-sibling::text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@class='tab-content']//p//text()[contains(.,'Bond')]", input_type="F_XPATH", get_num=True, split_list={"$":1}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='tab-content']//p//text()[contains(.,'Available')]", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='tab-content']//p//text()[contains(.,'ID')]", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//iframe/@src[contains(.,'map')]", input_type="F_XPATH", split_list={"center=":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//iframe/@src[contains(.,'map')]", input_type="F_XPATH", split_list={"center=":1,",":1})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[@class='details-container']/h3/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@class='details-container']/span[contains(.,'Office')]/a/text()", input_type="F_XPATH")
        
        yield item_loader.load_item()