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
import dateparser

class MySpider(Spider):
    name = 'christieuk_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://www.christieuk.com/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areadata=&areaname=&radius=&bedrooms=&minprice=&maxprice="]

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='fdLink']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status = response.xpath("//div[@class='status']/text()").get()
        if status and status.lower().strip() != "available":
            return
        
        item_loader.add_value("external_link", response.url)

        summary = "".join(response.xpath("//div[contains(@class,'tabShowHide expand')]/p/text()").getall())
        if summary and ("apartment" in summary.lower() or "flat" in summary.lower() or "maisonette" in summary.lower()):
            item_loader.add_value("property_type", "apartment")
        elif summary and "house" in summary.lower():
             item_loader.add_value("property_type", "house")
        elif summary and "studio" in summary.lower():
             item_loader.add_value("property_type", "studio")
        elif summary and "students only" in summary.lower():
             item_loader.add_value("property_type", "student_apartment")
        else:
            return

        item_loader.add_value("external_source", "Christieuk_PySpider_united_kingdom")

        external_id = response.url.split('/')[-1].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        address = response.xpath("//h1/text()").get()
        if address:
            count = address.count(",")
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[1].strip())
            if count>1:
                item_loader.add_value("zipcode", address.split(',')[-1].strip())
            
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip().replace('\xa0', ''))

        description = " ".join(response.xpath("//div[@class='tabShowHide expand']/p//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        room_count = response.xpath("//i[contains(@class,'bedroom')]/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//i[contains(@class,'bathrooms')]/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        rent = response.xpath("//h2/div/@data-bind/parent::div[contains(.,'Fee')]/text()").get()
        if rent:
            rent = rent.split('£')[-1].lower().split('pcm')[0].strip().replace(',', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'GBP')
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='property-photos-device1']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        floor = response.xpath("//li[contains(.,'FLOOR')]/text()").get()
        if floor:
            floor = "".join(filter(str.isnumeric, floor.lower().split('floor')[0].strip()))
            if floor:
                item_loader.add_value("floor", floor)

        furnished = response.xpath("//li[contains(.,'FULLY FURNISHED')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        terrace = response.xpath("//li[contains(.,'TERRACE')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_name", "CHRISTIES")
        item_loader.add_value("landlord_phone", "02392830888")
        item_loader.add_value("landlord_email", "lettings@christieuk.com")

        yield item_loader.load_item()
