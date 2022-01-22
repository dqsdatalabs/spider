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
    name = 'archliving_co_uk'
    execution_type = "testing"
    country = "united_kingdom"
    locale = "en"
    thousand_separator = ','
    scale_separator = '.'  
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.archliving.co.uk/search/{}.html?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Apartment",
                    "https://www.archliving.co.uk/search/{}.html?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Apartment+-+Purpose+Built",
                    "https://www.archliving.co.uk/search/{}.html?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Flat",
                    "https://www.archliving.co.uk/search/{}.html?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Flat+-+Conversion",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.archliving.co.uk/search/{}.html?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Bungalow+-+Semi+Detached",
                    "https://www.archliving.co.uk/search/{}.html?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=House",
                    "https://www.archliving.co.uk/search/{}.html?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=House+-+Detached",
                    "https://www.archliving.co.uk/search/{}.html?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=House+-+End+Terrace",
                    "https://www.archliving.co.uk/search/{}.html?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=House+-+Mid+Terrace",
                    "https://www.archliving.co.uk/search/{}.html?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=House+-+Semi-Detached",
                    "https://www.archliving.co.uk/search/{}.html?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=House+-+Terraced",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.archliving.co.uk/search/{}.html?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Studio",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='property']/div/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})   
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        ext_id = response.url.split("property-details/")[1].split("/")[0].strip()
        if ext_id:
            item_loader.add_value("external_id", ext_id)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Archliving_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div/h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div/h1/text()", input_type="F_XPATH",split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div/h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@id='property-long-description']//div[@class='col-md-8']/p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='room-icons']/span[1]/strong/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@class='room-icons']/span[2]/strong/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent_string", input_value="//div/h2/text()", input_type="F_XPATH")        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='property-carousel']/div[@class='carousel-inner']/div/img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//div[@id='property-floorplans']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div[@class='property-bullets']//li[contains(.,'FLOOR') and not(contains(.,'FLOORING'))]//text()", input_type="F_XPATH",split_list={"FLOOR":0})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[@type='application/ld+json']/text()[contains(.,'latitude') and contains(.,'longitude')]", input_type="F_XPATH",split_list={'latitude": "':1,'"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[@type='application/ld+json']/text()[contains(.,'latitude') and contains(.,'longitude')]", input_type="F_XPATH",split_list={'longitude": "':1,'"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@class='property-bullets']//li[contains(.,'Parking') or contains(.,'GARAGE') or contains(.,'PARKING')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//div[@class='property-bullets']//li[contains(.,'Washing Machine')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@class='property-bullets']//li[contains(.,'UNFURNISHED')]//text()", input_type="F_XPATH", tf_item=True, tf_value=False)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@class='property-bullets']//li[contains(.,'FURNISHED')]//text()", input_type="F_XPATH", tf_item=True, tf_value=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0116 270 6699", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Arch Living", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@archliving.co.uk", input_type="VALUE")
        item_loader.add_value("external_id", response.url.split("details/")[1].split("/")[0])

        yield item_loader.load_item()