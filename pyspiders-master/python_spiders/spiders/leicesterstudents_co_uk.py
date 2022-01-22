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
    name = 'leicesterstudents_co_uk'
    execution_type='testing'
    country='united_kingdom' 
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.leicesterstudents.co.uk/search/?instruction_type=Letting&showsold%2Cshowstc=on&location=&property_type=Apartment", 
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.leicesterstudents.co.uk/search/?instruction_type=Letting&showsold%2Cshowstc=on&location=&property_type=Terraced",
                    "https://www.leicesterstudents.co.uk/search/?instruction_type=Letting&showsold%2Cshowstc=on&location=&property_type=Town+House" 
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(url=item,
                                callback=self.parse,
                                meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen=False
        for item in response.xpath("//a[contains(@class,'property')]"):
            status = "".join(item.xpath(".//div[contains(@class,'availability-banner')]//text()").getall())
            if not status:
                follow_url = item.xpath("./@href").get()
                yield Request(response.urljoin(follow_url), callback=self.populate_item, meta={'property_type':response.meta.get('property_type')})
                seen=True
        
        if page ==2 or seen:        
            f_url = response.url.replace(f"search/{page-1}.html?", f"search/?").replace(f"search/?", f"search/{page}.html?")
            yield Request(f_url, callback=self.parse, meta={"page": page+1, 'property_type':response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Leicesterstudents_Co_PySpider_united_kingdom")

        title = response.xpath("//span[contains(@itemprop,'name')]//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        externalid=response.url
        if externalid:
            externalid=externalid.split("leicester-")[-1] 
            item_loader.add_value("external_id",externalid)

        address = response.xpath("//span[contains(@itemprop,'name')]//text()").get()
        if address:
            room_count = address.split(",")[0]
            if "studio" in room_count.lower():
                item_loader.add_value("room_count", "1")
            elif "bed" in room_count.lower():
                room_count = room_count.strip().split(" ")[0]
                item_loader.add_value("room_count", room_count)
            address = address.split(room_count)[-1].strip()
            city = address.split(",")[-1].strip()
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address.replace(",","").strip())
            item_loader.add_value("city", city)
        zipcode=response.xpath("//input[@name='property']/@value").get()
        if zipcode:
            zipcode=zipcode.split("-")[0].strip().split(" ")[-2:]
            item_loader.add_value("zipcode",zipcode)

        rent = "".join(response.xpath("//span[contains(@itemprop,'price')]/text()").getall())
        if rent:
            rent = rent.strip().replace("£","").replace("pppw","")
            rent = int(rent)*4
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
    

        desc = " ".join(response.xpath("//div[contains(@class,'details')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[contains(@id,'property-thumbnails')]//div[contains(@class,'carousel-inner')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//li[contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//li[contains(.,'furnished')]//text()[not(contains(.,'Unfurnished') or contains(.,'unfurnished'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        latitude_longitude = response.xpath("//script[contains(.,'googlemap')]//text()").get()
        if latitude_longitude:
            latitude_longitude = latitude_longitude.split('googlemap", "')[1].split('"')[0]
            latitude = latitude_longitude.split('&q=')[1].split('%2C')[0]
            longitude = latitude_longitude.split('&q=')[1].split('%2C')[1].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Leicester Student Lettings")
        item_loader.add_value("landlord_phone", "0116 255 13 33 ")
        item_loader.add_value("landlord_email", "info@leicesterstudents.co.uk")
        
        yield item_loader.load_item()