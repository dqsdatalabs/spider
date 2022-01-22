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
    name = 'markwebsterandco_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://markwebsterandco.co.uk/property-search/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=22&view=&pgp=",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "http://markwebsterandco.co.uk/property-search/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=9&view=&pgp=",
                    "http://markwebsterandco.co.uk/property-search/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=18&view=&pgp="
                    
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
        for item in response.xpath("//div[@class='large-thumbnail']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            status = item.xpath("./div/text()").get()
            if "agreed" in status.lower() or "under" in status.lower():
                continue
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={"property_type": response.meta.get('property_type')}
            )
        
        next_page = response.xpath("//a[@aria-label='Next']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type": response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Markwebsterandco_Co_PySpider_united_kingdom")

        title = " ".join(response.xpath("//h3//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'title')]//div[contains(@class,'details')]//text()").get()
        if address:
            city = address.split(",")[-1].strip()
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city)

        rent = response.xpath("//div[contains(@class,'price')]//text()").get()
        if rent:
            rent = rent.split("£")[1].strip().split(" ")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        deposit = response.xpath("//div[contains(@class,'summary')]//p//text()[contains(.,'deposit of')] | //div[contains(@class,'description')]//p//text()[contains(.,'deposit of')]").get()
        if deposit:
            deposit = deposit.split("£")[1].strip().split(" ")[0]
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[contains(@class,'summary')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = " ".join(response.xpath("//h3//text()").getall())
        if room_count:
            room_count = room_count.split("Bed")[0].strip().split(" ")[-1]
            item_loader.add_value("room_count", room_count)
        
        images = [x for x in response.xpath("//ul[contains(@class,'slides')]//@srcset").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Mark Webster Estate Agents")
        item_loader.add_value("landlord_phone", "01827 720777")
        item_loader.add_value("landlord_email", "lettings@markwebsterandco.co.uk")

        yield item_loader.load_item()