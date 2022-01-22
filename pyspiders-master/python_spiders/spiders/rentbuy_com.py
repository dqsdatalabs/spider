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
    name = 'rentbuy_com'
    execution_type='testing'
    country='australia'
    locale='en'
    start_urls = ['http://rentbuy.com/properties/rent/residential']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 1)
        
        seen = False
        for item in response.xpath("//div[@class='listing-box']"):
            follow_url = response.urljoin(item.xpath("./div[contains(@class,'thumbnail')]//@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 1 or seen:
            url = f"http://rentbuy.com/properties/rent/residential?page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        
        property_type = response.xpath("normalize-space(//div[@class='sale-rent-title']/text())").get()
        if "house" in property_type.lower() or "other" in property_type.lower():
            item_loader.add_value("property_type", "house")
        elif "apartment" in property_type.lower() or "unit" in property_type.lower():
            item_loader.add_value("property_type", "apartment")
        else: return
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Rentbuy_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="normalize-space(//p[@class='address']/text())", input_type="F_XPATH", split_list={",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="normalize-space(//p[@class='address']/text())", input_type="F_XPATH", split_list={",":1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="normalize-space(//span[@class='price']/text())", input_type="F_XPATH", get_num=True, per_week=True, split_list={"$":1," ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="USD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'Bedroom')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(.,'Bathroom')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li[contains(.,'Bond')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        
        address = " ".join(response.xpath("//div[@class='property-title']//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
        
        desc = " ".join(response.xpath("//div[@class='basic-description']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        if "floor" in desc.lower():
            floor = desc.lower().split("floor")[0].strip().split(" ")[-1]
            if "mid " not in floor:
                item_loader.add_value("floor", floor)
                
        from datetime import datetime
        import dateparser
        if "available now" in desc.lower():
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        
        match = re.search(r'(\d+/\d+/\d+)', desc)
        if match:
            newformat = dateparser.parse(match.group(1), languages=['en']).strftime("%Y-%m-%d")
            item_loader.add_value("available_date", newformat)
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'detail-slider')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lat:":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lng:":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking')]/text()[.!='0'] | //div[@class='basic-description']//p//text()[contains(.,'parking') or contains(.,'Parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[@class='basic-description']//p//text()[contains(.,'Balcony') or contains(.,'balcony')][not(contains(.,'No balcony'))] | //li[contains(.,'Balcony')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[@class='agent-detail']/h2//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@class='agent-detail']//a[contains(@href,'tel')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="auburn@rentbuy.com", input_type="VALUE")
        
        yield item_loader.load_item()