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
    name = 'fssproperty_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.fssproperty.co.uk/search/1.html?department=Residential&instruction_type=Letting&address_keyword=&minprice=&maxprice=&category=apartment",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.fssproperty.co.uk/search/1.html?department=Residential&instruction_type=Letting&address_keyword=&minprice=&maxprice=&category=detached",
                    "https://www.fssproperty.co.uk/search/1.html?department=Residential&instruction_type=Letting&address_keyword=&minprice=&maxprice=&category=semi_detached",
                    "https://www.fssproperty.co.uk/search/1.html?department=Residential&instruction_type=Letting&address_keyword=&minprice=&maxprice=&category=town_house",
                    "https://www.fssproperty.co.uk/search/1.html?department=Residential&instruction_type=Letting&address_keyword=&minprice=&maxprice=&category=bungalow",
                    "https://www.fssproperty.co.uk/search/1.html?department=Residential&instruction_type=Letting&address_keyword=&minprice=&maxprice=&category=farm_house",
                    "https://www.fssproperty.co.uk/search/1.html?department=Residential&instruction_type=Letting&address_keyword=&minprice=&maxprice=&category=maisonettes",
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

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@id='search-results']/div[@class='property']//a[contains(.,'More details')]/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        if page == 2 or seen:
            yield Request(f"https://www.fssproperty.co.uk/search/{page}.html?" + response.url.split('html?')[1], callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Fssproperty_Co_PySpider_united_kingdom", input_type="VALUE")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        external_id = response.url.split("property-details/")[1].split("/")[0]
        item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//h4/text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span/img[contains(@src,'bed')]/@alt", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span/img[contains(@src,'bath')]/@alt", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//script[contains(.,'address')]/text()", input_type="F_XPATH", split_list={'streetAddress": "':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'address')]/text()", input_type="F_XPATH", split_list={'latitude": "':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'address')]/text()", input_type="F_XPATH", split_list={'longitude": "':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h1//text()", input_type="F_XPATH", get_num=True, split_list={"pcm":0}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        
        desc = " ".join(response.xpath("//div[contains(@class,'card-body')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        address = response.xpath("//script[contains(.,'address')]/text()").get()
        if address:
            zipcode = address.split('streetAddress": "')[1].split('"')[0].split(",")[-1].strip()
            if not zipcode.split(" ")[0].isalpha():
                item_loader.add_value("zipcode", zipcode)
                city = address.split(zipcode)[0].strip().strip(",").split(",")[-1].strip()
                item_loader.add_value("city", city)
            else:
                item_loader.add_value("city", zipcode)

        
        from datetime import datetime
        import dateparser
        if "available" in desc.lower():
            available_date = desc.lower().split("available")[1].split("furnished")[0].replace(" un","").replace(".","")
            if "now" in available_date or "immediately" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date.replace("early","").strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                        
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,' furnished') or contains(.,'Furnished')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'parking') or contains(.,'garage') or contains(.,'Parking')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[contains(.,'Floor')]/text()", input_type="F_XPATH", split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='carousel-inner']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//div[contains(@id,'floorplans')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Feather Smailes Scales LLP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@class='details-call-tel']/a/@href", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@fssproperty.co.uk", input_type="VALUE")
        
        yield item_loader.load_item()