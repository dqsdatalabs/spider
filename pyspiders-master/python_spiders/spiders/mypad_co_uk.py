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
    name = 'mypad_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.mypad.co.uk/propertysearch/?marketing_flag=100&department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&property_type=54&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&bedrooms=&view=&pgp=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.mypad.co.uk/propertysearch/?marketing_flag=100&department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&property_type=41&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&bedrooms=&view=&pgp=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.mypad.co.uk/propertysearch/?marketing_flag=100&department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&property_type=101&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&bedrooms=&view=&pgp=",
                ],
                "property_type" : "room"
            },
            {
                "url" : [
                    "https://www.mypad.co.uk/propertysearch/?marketing_flag=99&department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&property_type=&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&bedrooms=&view=&pgp=",
                ],
                "property_type" : "student_apartment"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[contains(@class,'properties')]/li//a[contains(.,'More Details')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='next page-numbers']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Mypad_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='property_meta']//text()[contains(.,'Ref')]", input_type="F_XPATH", split_list={"Ref:":1, " ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='property_meta']//text()[contains(.,'Bedroom')]", input_type="F_XPATH", get_num=True, split_list={"Bedrooms:":1, " ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@class='property_meta']//text()[contains(.,'Bathroom')]", input_type="F_XPATH", get_num=True, split_list={"Bathrooms:":1, " ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@class='property_meta']//text()[contains(.,'Furnished: Furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='property_meta']//text()[contains(.,'Available:')]", input_type="F_XPATH", split_list={"Available:":1})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@class='features']//text()[contains(.,'PARKING')] | //div[@class='summary-contents']//text()[contains(.,' parking included')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="pets_allowed", input_value="//div[@class='features']//text()[contains(.,'  PETS ALLOWED')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":1, ")":0})
        
        desc = " ".join(response.xpath("//div[@class='summary-contents']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            
        rent = response.xpath("//div[@class='price']//text()").get()
        if rent:
            if "pw" in rent:
                ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='price']//text()", input_type="F_XPATH", get_num=True, per_week=True, split_list={"pw":0, "£":1})
            else:
                ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='price']//text()", input_type="F_XPATH", get_num=True, split_list={"pcm":0, "£":1})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        city = response.xpath("//h1/text()").get()
        if city:
            if "," in city:
                item_loader.add_value("city", city.split(",")[-1].strip())
            elif "/" in city:
                item_loader.add_value("city", city.split("/")[-1].strip())
            elif "-" in city:
                item_loader.add_value("city", city.split("-")[-1].strip())
            else:
                item_loader.add_value("city", city.strip())
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1].replace("-","")
            if not "all" in floor:
                item_loader.add_value("floor", floor)
           
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='slider']/ul/li//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="MYPAD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01482 342445", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@mypad.co.uk", input_type="VALUE")
        
        yield item_loader.load_item()