# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
from datetime import datetime
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'coogeerealestate_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.coogeerealestate.com.au/search-results/{}?property_type%5B%5D=Apartment&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.coogeerealestate.com.au/search-results/{}?property_type%5B%5D=Unit&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "house",
            },
            {
                "url" : [
                    "https://www.coogeerealestate.com.au/search-results/{}?property_type%5B%5D=Studio&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(""),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base":item})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'container')]/a"):
            status = item.xpath("./following-sibling::div[@class='sticker']/a/text()").get()
            if status and ("under" in status.lower() or "let" in status.lower()):
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            base = response.meta["base"]
            slug = f"page/{page}/"
            p_url = base.format(slug)
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "base":base, "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        status = response.xpath("//label[contains(.,'Contract')]/following-sibling::div/text()[contains(.,'AUC') or contains(.,'Auction')]").get()
        if status:
            return
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Coogeerealestate_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1//text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[contains(@class,'desc')]/h3/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='detail-description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//label[contains(.,'Land Size')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//label[contains(.,'Bedroom')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//label[contains(.,'Bathroom')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//label[contains(.,'Price')]/following-sibling::div/text()[not(contains(.,'AUC'))]", input_type="F_XPATH", get_num=True, per_week=True, split_list={" ":0,".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//label[contains(.,'Available')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//label[contains(.,'Bond')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//label[contains(.,'ID')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='image']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//div[contains(@class,'floorplan')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'L.marker([')]/text()", input_type="F_XPATH", split_list={"L.marker([":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'L.marker([')]/text()", input_type="F_XPATH", split_list={"L.marker([":1, ",":1, "]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="pets_allowed", input_value="//li[contains(.,'Pet Friendly')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//label[contains(.,'Parking')]/following-sibling::div[.!='0'] | //label[contains(.,'Car')]/following-sibling::div[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Lift')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//li[contains(.,'Pool')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//strong/p/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//i[contains(@class,'phone')]/following-sibling::text()", input_type="F_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="", input_type="VALUE")

        yield item_loader.load_item()
