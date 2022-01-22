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
    name = 'spencerharvey_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.spencerharvey.co.uk/search/?showstc=on&instruction_type=Letting&address_keyword=&property_type=Apartment&minprice=&maxprice=",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.spencerharvey.co.uk/search/?showstc=on&instruction_type=Letting&address_keyword=&property_type=Bungalow&minprice=&maxprice=",
                    "https://www.spencerharvey.co.uk/search/?showstc=on&instruction_type=Letting&address_keyword=&property_type=Detached&minprice=&maxprice=",
                    "https://www.spencerharvey.co.uk/search/?showstc=on&instruction_type=Letting&address_keyword=&property_type=Semi-Detached&minprice=&maxprice=",
                    "https://www.spencerharvey.co.uk/search/?showstc=on&instruction_type=Letting&address_keyword=&property_type=Terraced&minprice=&maxprice=",
                    "https://www.spencerharvey.co.uk/search/?showstc=on&instruction_type=Letting&address_keyword=&property_type=Town+House&minprice=&maxprice=",
                    "https://www.spencerharvey.co.uk/search/?showstc=on&instruction_type=Letting&address_keyword=&property_type=Villa&minprice=&maxprice=",
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
                
        for item in response.xpath("//div[@class='page-content']//a[@class='property-image']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Spencerharvey_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("details/")[1].split("/")[0])

        title = " ".join(response.xpath("//h1[contains(@itemprop,'name')]/text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = "".join(response.xpath("//h1[contains(@itemprop,'name')]/text()").getall())
        if address:
            city = address.split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)

        rent = response.xpath("//span[contains(@itemprop,'price')]//strong/text()").get()
        if rent:
            rent = rent.split("£")[1].split(" ")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@itemprop,'description')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//span[contains(@class,'property-bedrooms')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//span[contains(@class,'property-bathrooms')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'property-thumbnails')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//img[contains(@alt,'Floorplan')]//@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        parking = response.xpath("//div[contains(@itemprop,'description')]//p//text()[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//div[contains(@itemprop,'description')]//p//text()[contains(.,'Furnished') or contains(.,'FURNISHED')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        floor = response.xpath("//div[contains(@itemprop,'description')]//p//text()[contains(.,'Floor')]").get()
        if floor:
            floor = floor.split("Floor")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor.strip())

        energy_label = response.xpath("//div[contains(@itemprop,'description')]//p//text()[contains(.,'EPC')]").get()
        if energy_label:
            if ":" in energy_label:
                energy_label = energy_label.split(":")[1].strip()
            else:
                energy_label = energy_label.replace("TO FOLLOW","").replace("Rating","").split(" ")[-1]
            item_loader.add_value("energy_label", energy_label)

        latitude_longitude = response.xpath("//script[contains(.,'googlemap')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('&q=')[1].split('%2C')[0]
            longitude = latitude_longitude.split('%2C')[1].split('"')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "SPENCER HARVEY")
        item_loader.add_value("landlord_phone", "+44 (0)161 480 8888")
        item_loader.add_value("landlord_email", "info@spencerharvey.co.uk ")

        status = response.xpath("//img[contains(@alt,'{$availability}')]//@src[contains(.,'let-agreed')]").get()
        if status:
            return
        else:
            yield item_loader.load_item()