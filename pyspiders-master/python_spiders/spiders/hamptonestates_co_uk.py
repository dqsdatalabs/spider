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
    name = 'hamptonestates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.hamptonestates.co.uk/search?sta=toLet&st=rent&pt=residential&stygrp=3",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.hamptonestates.co.uk/search?sta=toLet&st=rent&pt=residential&stygrp=2&stygrp=8&stygrp=9&stygrp=6",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.hamptonestates.co.uk/search?sta=toLet&st=rent&pt=residential&stygrp=7",
                ],
                "property_type" : "room"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='property-list']/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='paging-next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Hamptonestates_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("/")[-1])

        address = " ".join(response.xpath("//h1[contains(@class,'address')]//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
            item_loader.add_value("title", address)
        city = response.xpath("//div[contains(@class,'property-address')]//span[contains(@class,'locality')][1]//text()").get()
        if city:
            city = city.replace(",","").strip()
            item_loader.add_value("city", city)
        zipcode = response.xpath("//div[contains(@class,'property-address')]//span[contains(@class,'postcode')]//text()").get()
        if zipcode:
            zipcode = zipcode.replace(",","").strip()
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//td[contains(.,'Rent')]/following-sibling::td//span[contains(@class,'price')]//text()").get()
        if rent:
            rent = rent.strip().replace(",","").split("£")[1]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        room_count = response.xpath("//td[contains(.,'Bed')]/following-sibling::td//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//td[contains(.,'Bath')]/following-sibling::td//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        furnished = response.xpath("//td[contains(.,'Furnished')]/following-sibling::td//text()").get()
        if furnished:
            if "Unfurnished" not in furnished:
                item_loader.add_value("furnished", True)
            
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//td[contains(.,'Available')]/following-sibling::td//text()").getall())
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                available_date = available_date.strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        desc = " ".join(response.xpath("//div[contains(@class,'property-description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        deposit = response.xpath("//td[contains(.,'Deposit')]/following-sibling::td/span/text()").get()
        if deposit:
            deposit = deposit.strip().split(",")[0].split(".")[0]
            item_loader.add_value("deposit", deposit)

        features = "".join(response.xpath("//div[@class='property-features']//li//text()").getall())
        if features:
            if "parking" in features.lower():
                item_loader.add_value("parking", True)

        
        images = [x for x in response.xpath("//a[contains(@rel,'photograph')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude = response.xpath("//meta[contains(@property,'latitude')]//@content").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        
        longitude = response.xpath("//meta[contains(@property,'longitude')]//@content").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_value("landlord_name", "Hampton Estates")
        item_loader.add_value("landlord_phone", "028 9064 2888")
        item_loader.add_value("landlord_email", "info@hamptonestates.co.uk")

        yield item_loader.load_item()