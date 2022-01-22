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
    name = 'poolerestateagents_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    }
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.poolerestateagents.com/search?sta=toLet&st=rent&pt=residential&stygrp=3",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.poolerestateagents.com/search?sta=toLet&st=rent&pt=residential&stygrp=2&stygrp=6",
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

        for item in response.xpath('//div[@class="PropBox-imgWrapper"]/a/@href').getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='paging-next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Poolerestateagents_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("/")[-1])

        title = "".join(response.xpath("//h1[contains(@class,'Address')]//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            item_loader.add_value("address", title)
        
        city = response.xpath("//span[contains(@class,'addressTown')]//text()").get()
        if city:
            item_loader.add_value("city", city.replace(",", ""))
        zipcode = response.xpath("//div[contains(@class,'ListingHeadline-address')]//span[contains(@class,'Address-addressOutcode')]//text()").get()
        zipcode1 = response.xpath("//div[contains(@class,'ListingHeadline-address')]//span[contains(@class,'Address-addressIncode')]//text()").get()
        if zipcode and zipcode1:
            item_loader.add_value("zipcode", zipcode + " " + zipcode1)

        rent = response.xpath("//span[contains(@class,'price-text')]//text()").get()
        if rent:
            rent = rent.split("£")[1]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        from datetime import datetime
        import dateparser
        available_date = response.xpath("//th[contains(.,'Available')]/following-sibling::td//text()").get()
        if available_date:
            available_date = available_date.strip()
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        furnished = response.xpath("//th[contains(.,'Furnished')]/following-sibling::td//text()").get()
        if furnished:
            if "Unfurnished" not in furnished:
                item_loader.add_value("furnished", True)
            
        room_count = response.xpath("//th[contains(.,'Bed')]/following-sibling::td//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        desc = " ".join(response.xpath("//div[contains(@class,'ListingDescr-text')]//text()").getall())
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
        
        item_loader.add_value("landlord_name", "POOLER ESTATE AGENTS")
        item_loader.add_value("landlord_phone", "028 9045 3319")
        item_loader.add_value("landlord_email", "info@poolerestateagents.com")

        yield item_loader.load_item()