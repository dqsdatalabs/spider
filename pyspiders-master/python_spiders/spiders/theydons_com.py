# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from word2number import w2n
from datetime import datetime
from datetime import date
import dateparser
import re

class MySpider(Spider):
    name = 'theydons_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source = "Theydons_PySpider_united_kingdom"
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.theydons.com/search-results/?department=residential-lettings&address_keyword=&radius=&property_type=22&minimum_bedrooms=&minimum_price=&maximum_price=",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.theydons.com/search-results/?department=residential-lettings&address_keyword=&radius=&property_type=9&minimum_bedrooms=&minimum_price=&maximum_price=",
                    "https://www.theydons.com/search-results/?department=residential-lettings&address_keyword=&radius=&property_type=18&minimum_bedrooms=&minimum_price=&maximum_price="
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )
    
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//ul[contains(@class,'properties')]/li"):
            status = item.xpath(".//div[contains(@class,'let')]/text()").get()
            if not status or "let agreed" not in status.lower():
                follow_url = item.xpath(".//a/@href").get()
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(@class,'next page-numbers')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_source", self.external_source)

        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            zipcode = address.split(',')[-1].strip()
            if len(zipcode.split(" "))>1:
                item_loader.add_value("zipcode", zipcode.split(' ')[-1])
                item_loader.add_value("city"," ".join(zipcode.split(' ')[:-1]))
            else:
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("city", address.split(',')[-2].strip())
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@class='summary-contents']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.replace('\xa0', '')))

        room_count = response.xpath("//i[contains(@class,'bed')]/following-sibling::text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//i[contains(@class,'bath')]/following-sibling::text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            if bathroom_count.isdigit():
                item_loader.add_value("bathroom_count", bathroom_count)

        rent = response.xpath("//div[@class='price']/text()").get()
        if rent:
            rent = rent.strip().split(' ')[0].replace(',', '').replace('£', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'GBP')
        
        images = [response.urljoin(x) for x in response.xpath("//ul[@class='slides']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = [x for x in response.xpath("//div[@class='floorplans']//@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        energy_label = response.xpath("//div[@class='epcs']//@src").get()
        if energy_label:
            energy_label = energy_label.split('EPC-')[1].split("-")[0].strip()
            if "." in energy_label: energy_label = energy_label.split(".")[0]
            item_loader.add_value("energy_label", energy_label)
        
        parking = response.xpath("//li[contains(.,'parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//li[contains(.,'unfurnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", False)
        else:
            furnished = response.xpath("//li[contains(.,'furnished')]/text() | //li[contains(.,'Furnished')]/text()").get()
            if furnished:
                item_loader.add_value("furnished", True)

        elevator = response.xpath("//li[contains(.,'elevator')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//li[contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        washing_machine = response.xpath("//li[contains(.,'washing machine')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_value("landlord_name", "Theydons")
        item_loader.add_value("landlord_phone", "+44 (0)20 3972 2001")
        item_loader.add_value("landlord_email", "info@theydons.com")
        
        yield item_loader.load_item()