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
    name = 'causewaycoastsales_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source = "Causewaycoastsales_PySpider_united_kingdom"
    custom_settings = {
        "PROXY_ON" : True,
    }
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.causewaycoastsales.com/search?sta=toLet&st=rent&pt=residential&pt=commercial&stygrp=3",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.causewaycoastsales.com/search?sta=toLet&st=rent&pt=residential&pt=commercial&stygrp=2&stygrp=10&stygrp=8&stygrp=9&stygrp=6",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@class='property-list']/li/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='paging-next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.url.split("/")[-1])

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)
            item_loader.add_value("address", title)

        address = " ".join(response.xpath("//h1//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
        
        city = response.xpath("//span[contains(@class,'address')]//span[contains(@class,'locality')]//text()").get()
        if city:
            item_loader.add_value("city", city)
        
        zipcode = response.xpath("//span[contains(@class,'address')]//span[contains(@class,'postcode')]//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(",")[1].strip())

        deposit = response.xpath("//tr[contains(.,'Deposit')]//td//text()").get()
        if deposit:
            deposit = deposit.replace(",","").replace("£","").strip()
            item_loader.add_value("deposit", int(float(deposit)) )

        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//tr[contains(.,'Available')]//td//text()").getall())
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                available_date = available_date.strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            
        rent = response.xpath("//span[contains(@class,'price-value')]//text()").get()
        if rent:
            rent = rent.split("£")[1].replace(",","").strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//tr[contains(.,'Bed')]//td//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//tr[contains(.,'Bath')]//td//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        furnished = response.xpath("//tr[contains(.,'Furnished')]//td//text()[contains(.,' furnished') or contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        desc = " ".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        images = [x for x in response.xpath("//a[contains(@rel,'photograph')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude = response.xpath("//meta[contains(@property,'latitude')]//@content").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        
        longitude = response.xpath("//meta[contains(@property,'longitude')]//@content").get()
        if longitude:
            item_loader.add_value("longitude", longitude)

        item_loader.add_value("landlord_name", "CAUSEWAY COAST SALES")
        item_loader.add_value("landlord_phone", "028 7083 2220")
        item_loader.add_value("landlord_email", "info@causewaycoastsales.co.uk")

        yield item_loader.load_item()