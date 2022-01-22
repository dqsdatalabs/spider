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
    name = 'elitestlucia_com'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Elitestlucia_PySpider_australia'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.elitestlucia.com/rent?search=&listing_type=rent&property_type=Apartment&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.elitestlucia.com/rent?search=&listing_type=rent&property_type=Duplex&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.elitestlucia.com/rent?search=&listing_type=rent&property_type=Flat&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.elitestlucia.com/rent?search=&listing_type=rent&property_type=Flat&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.elitestlucia.com/rent?search=&listing_type=rent&property_type=House&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.elitestlucia.com/rent?search=&listing_type=rent&property_type=Townhouse&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.elitestlucia.com/rent?search=&listing_type=rent&property_type=Unit&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.elitestlucia.com/rent?search=&listing_type=rent&property_type=Semi-detached&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.elitestlucia.com/rent?search=&listing_type=rent&property_type=Terrace&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.elitestlucia.com/rent?search=&listing_type=rent&property_type=Villa&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                ],
                "property_type" : "house",
            },
            {
                "url" : [
                    "http://www.elitestlucia.com/rent?search=&listing_type=rent&property_type=Studio&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='card']"):
            status = item.xpath(".//div[@class='figType']/text()").get()
            if status and ("leased" in status.lower() or "under" in status.lower()):
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("id=")[1])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value=self.external_source, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[contains(@class,'address')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[contains(@class,'address')]//text()", input_type="F_XPATH", split_list={",":-1})

        zipcode = response.xpath("//script[contains(.,'props = [')]//text()").get()
        if zipcode:
            zipcode = zipcode.split('address":"')[1].split('"')[0].split(",")[-2].strip()
            item_loader.add_value("zipcode", zipcode)

        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1[contains(@class,'pageTitle')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'description')]//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(@class,'bed')]//following-sibling::div//text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(@class,'icon-drop')]//following-sibling::div//text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        
        price= response.xpath("//div[contains(@class,'listPrice')]/text()").get()
        if price and "$" in price:
            price = price.split("$")[1].strip()
            if " " in price:
                rent = price.split(" ")[0].replace("p/w","").replace("P/W","")
            else:
                rent = price.split("p")[0].replace("p/w","").replace("P/W","")
            item_loader.add_value("rent", int(float(rent))*4)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")

        furnished = "".join(response.xpath("//div[contains(@class,'listPrice')]/text() | //h1[contains(@class,'pageTitle')]//text()").getall())
        if furnished and "furnished" in furnished.lower() and "unfurnished" not in furnished.lower():
            item_loader.add_value("furnished", True)
        else:
            furnished = "".join(response.xpath("//div[contains(@class,'description')]//p//text()[contains(.,'FURNISHED')]").getall())
            if furnished and "furnished" in furnished.lower() and "unfurnished" not in furnished.lower():
                item_loader.add_value("furnished", True)

        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//a[contains(@class,'galleryItem')]//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'position')]/text()", input_type="F_XPATH", split_list={'lat":':1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'position')]/text()", input_type="F_XPATH", split_list={'lng":':1,"}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//span[contains(@class,'car')]//following-sibling::div//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[contains(@class,'amItem')][contains(.,'Balcony')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//div[contains(@class,'amItem')][contains(.,'Swimming Pool')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//div[contains(@class,'amItem')][contains(.,'Dishwasher')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="ELITE REAL ESTATE", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0447724067", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="admin@elitestlucia.com", input_type="VALUE")
        
        yield item_loader.load_item()
