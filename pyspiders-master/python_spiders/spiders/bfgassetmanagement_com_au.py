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
    name = 'bfgassetmanagement_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.bfgassetmanagement.com.au/search/suburb:/property_type:8/building_type:3/bed_min:/bed_max:/price_min:/price_max:/",
                    
                ],
                "property_type" : "apartment"
            },
            {
                "url": [
                    "https://www.bfgassetmanagement.com.au/search/suburb:/property_type:8/building_type:1/bed_min:/bed_max:/price_min:/price_max:/",
                    "https://www.bfgassetmanagement.com.au/search/suburb:/property_type:8/building_type:5/bed_min:/bed_max:/price_min:/price_max:/",
                    "https://www.bfgassetmanagement.com.au/search/suburb:/property_type:8/building_type:2/bed_min:/bed_max:/price_min:/price_max:/"
                ],
                "property_type" : "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(url=item,
                                callback=self.parse,
                                meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='property_list']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type':response.meta.get('property_type')})
#     # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Bfgassetmanagement_PySpider_australia")
        item_loader.add_value("external_id", response.url.split("property/")[1].split("_")[0])

        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//h1//text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            city_zipcode = address.split(",")[-1].strip()
            zipcode = city_zipcode.split("\u00a0")[-1]
            city = city_zipcode.split(zipcode)[0].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//div[contains(@class,'price_pp')]//text()").get()
        if rent:
            rent = rent.strip().replace("$","").split(".")[0]
            rent = int(rent)*4
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "AUD")

        deposit = response.xpath("//div[contains(@class,'price_pcm')]//text()[contains(.,'bond')]").get()
        if deposit:
            deposit = deposit.replace("$","").strip().split(" ")[0]
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[contains(@class,'property_desc')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = "".join(response.xpath("//img[contains(@src,'bed')]//parent::div//div//text()").getall())
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = "".join(response.xpath("//img[contains(@src,'bath')]//parent::div//div//text()").getall())
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'propertyCarousel')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//img[contains(@src,'car')]//parent::div//div//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        item_loader.add_value("landlord_name", "BFG ASSET MANAGEMENT")
        item_loader.add_value("landlord_phone", "0498 999 644")
        item_loader.add_value("landlord_email", "admin@bfgam.com.au")
        yield item_loader.load_item()