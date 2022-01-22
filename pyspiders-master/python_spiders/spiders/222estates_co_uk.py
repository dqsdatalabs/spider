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
    name = '222estates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.222estates.co.uk/search/?showstc=on&showsold=on&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Flat",
                    "https://www.222estates.co.uk/search/?showstc=on&showsold=on&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Apartment",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.222estates.co.uk/search/?showstc=on&showsold=on&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Detached+House",
                    "https://www.222estates.co.uk/search/?showstc=on&showsold=on&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=End+of+Terrace+House",
                    "https://www.222estates.co.uk/search/?showstc=on&showsold=on&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=House+Share",
                    "https://www.222estates.co.uk/search/?showstc=on&showsold=on&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Semi-Detached+House",
                    "https://www.222estates.co.uk/search/?showstc=on&showsold=on&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Terraced+House",
                    "https://www.222estates.co.uk/search/?showstc=on&showsold=on&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Town+House",
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://www.222estates.co.uk/search/?showstc=on&showsold=on&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Studio+Flat",
                ],
                "property_type": "studio"
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
        
        for item in response.xpath("//a[@class='thumbnail']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//a[@rel='next']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "222estates_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("details/")[1].split("/")[0])

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//span[contains(@itemprop,'name')]//text()").get()
        if address:
            city = address.replace(",","").strip().split(" ")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)

        rent = response.xpath("//span[contains(@itemprop,'price')]//span/text()").get()
        if rent:
            rent = rent.strip().split("£")[1].strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        deposit = response.xpath("//h2[contains(.,'Key Features')]//following-sibling::ul//li[contains(.,'Deposit') or contains(.,'deposit')]//text()").get()
        if deposit:
            deposit = deposit.lower().split("deposit")[0].replace("£","").strip()
            item_loader.add_value("deposit", deposit)
        else:
            deposit = "".join(response.xpath("//div[contains(@class,'property-details')]//p//text()[contains(.,'deposit') or contains(.,'Deposit')]").getall())
            if deposit:
                deposit = deposit.split("deposit is £")[1].replace(".","")
                item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[contains(@class,'property-details')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//span[contains(@class,'property-bedrooms')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//span[contains(@class,'property-bathrooms')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'property-carousel')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking') or contains(.,'Garage')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//li[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//li[contains(.,'Lift')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("//li[contains(.,'Floor') or contains(.,'floor apartment')]//text()[not(contains(.,'Flooring') or contains(.,'flooring'))]").get()
        if floor:
            floor = floor.lower().split("floor")[0].strip()
            item_loader.add_value("floor", floor.strip())
            
        washing_machine = response.xpath("//li[contains(.,'Washing Machine')]//text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        latitude_longitude = response.xpath("//script[contains(.,'googlemap')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('&q=')[1].split('%2C')[0]
            longitude = latitude_longitude.split('&q=')[1].split('%2C')[1].split('"')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "222 Estates")
        item_loader.add_value("landlord_phone", "01925 499599")
        item_loader.add_value("landlord_email", "info@222estates.co.uk")

        yield item_loader.load_item()