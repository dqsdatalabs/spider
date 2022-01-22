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
    name = 'moveon_ie'
    execution_type='testing'
    country='ireland'
    locale='en'

    def start_requests(self):
        base_url = "https://www.moveon.ie/search"
        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'Origin': 'https://www.moveon.ie',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            'Referer': 'https://www.moveon.ie/search',
            'Accept-Language': 'tr,en;q=0.9',
        }
        infos = [
            {
                "payloads" : ["SearchSale=1&SearchType=1"],
                "property_type" : "apartment",
            },
            {
                "payloads" : ["SearchSale=1&SearchType=7", "SearchSale=1&SearchType=2", "SearchSale=1&SearchType=6", "SearchSale=1&SearchType=8", "SearchSale=1&SearchType=5", "SearchSale=1&SearchType=3"],
                "property_type" : "house",
            },
        ]
        for info in infos:
            for payload in info["payloads"]:
                yield Request(base_url,
                            method="POST",
                            dont_filter=True,
                            headers=headers,
                            body=payload,
                            callback=self.parse,
                            meta={'property_type': info['property_type']})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//section[@id='VenueListing']/a"):
            url = item.xpath("./@href").get()
            room_count = item.xpath(".//div[@class='Bedrooms']/p/text()").get()
            bathroom_count = item.xpath(".//div[@class='Bathrooms']/p/text()").get()            
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"],"room_count":room_count ,"bathroom_count":bathroom_count})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Moveon_PySpider_ireland")
        prop_type = response.xpath("//div[@class='ServiceName']/text()[contains(.,'Restaurant')]").get()
        if prop_type:
            return
        title = response.xpath("//div[@id='TitleBox']/h1/text()").get()
        if title:
            item_loader.add_value("title",re.sub("\s{2,}", " ", title).replace("\n"," "))
        address = response.xpath("//div[@id='TitleBox']/h1/text()").get()
        if address:
            item_loader.add_value("address",re.sub("\s{2,}", " ", address).replace("\n"," "))
            item_loader.add_value("city",address.split(",")[-1].strip())
        room_count = response.meta.get('room_count')
        if room_count:
            item_loader.add_value("room_count", room_count.split("Bedrooms")[0])
     
        bathroom_count = response.meta.get('bathroom_count')
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Bathroom")[0])
       
        rent = "".join(response.xpath("//div[@id='PriceBox']//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent) 
        square_meters = response.xpath("//div[@class='ServiceName']/text()[contains(.,'SQM')]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("SQM")[0])

        description = " ".join(response.xpath("//div[@id='MainDescription']/p//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.strip())

            if not item_loader.get_collected_values("square_meters") and "sq.m" in description:
                sqm = description.split("sq.m")[0].strip().split(" ")[-1].strip()
                if sqm.replace(".","").replace(",","").isnumeric():
                    item_loader.add_value("square_meters", sqm)

        energy_label = response.xpath("//div[h2[.='Certificates']]/following-sibling::div[1]/img/@alt").get()
        if energy_label:            
            item_loader.add_value("energy_label",energy_label.upper())
        images = [x for x in response.xpath("//div[@id='jssor_1']//div[@class='VenuePortImage']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        parking = response.xpath("//div[@class='ServiceName']/text()[contains(.,'Parking') or contains(.,'Car Park')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        item_loader.add_xpath("landlord_phone", "//div[@id='AgentDetails']/p[small[.='call:']]/text()")
        item_loader.add_xpath("landlord_name", "//div[@id='AgentDetails']/p[2]/text()")
        item_loader.add_xpath("landlord_email", "//div[@id='AgentDetails']/p[small[.='email:']]/text()")
        
        yield item_loader.load_item()