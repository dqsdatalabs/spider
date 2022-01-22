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
    name = 'quickletni_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.quickletni.co.uk/search?sta=toLet&sta=let&st=rent&pt=residential&stygrp=3",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.quickletni.co.uk/search?sta=toLet&sta=let&st=rent&pt=residential&stygrp=2&stygrp=10&stygrp=8&stygrp=9&stygrp=6",
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

        for item in response.xpath("//div[contains(@class,'PropBox-content')]"):
            url = item.xpath("./a/@href").get()
            status = item.xpath(".//div[@class='PropBox-status']/span/text()[.='Let']").get()
            if status:
                continue
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='Paging-next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Quickletni_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("/")[-1])

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)
        
        address = " ".join(response.xpath("//h1//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
        
        city = response.xpath("//h1//span[contains(@class,'addressTown')]//text()").get()
        if city:
            item_loader.add_value("city", city.replace(",",""))
        
        zipcode = " ".join(response.xpath("//h1//span[contains(@class,'addressPostcode')]//text()").getall())
        if zipcode:
            zipcode = re.sub('\s{2,}', ' ', zipcode.strip())
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//span[contains(@class,'price-text')]//text()").get()
        if rent:
            rent = rent.split("£")[1].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        deposit = response.xpath("//tr[contains(@class,'deposit')]//td//text()").get()
        if deposit:
            deposit = deposit.strip().split("£")[1].replace(",","")
            item_loader.add_value("deposit", deposit)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//tr[contains(@class,'availablefrom')]//td//text()").getall())
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        furnished = response.xpath("//tr[contains(@class,'furnished')]//td//text()").get()
        if furnished:
            if "Furnished" in furnished:
                item_loader.add_value("furnished", True)
        
        room_count = response.xpath("//tr[contains(@class,'bed')]//td//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//tr[contains(@class,'bath')]//td//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        desc = " ".join(response.xpath("//div[contains(@class,'ListingDescr')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1].replace("(","").replace(")","")
            not_list = ["main","laminate","under"]
            status = True
            for i in not_list:
                if i in floor.lower():
                    status = False
            if status:
                item_loader.add_value("floor", floor.replace("-","").upper())

        parking = response.xpath("//div[contains(@class,'ListingBullets')]//li[contains(.,'Garage')]//text()").get()
        if parking:
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

        item_loader.add_value("landlord_name", "QUICKLET")
        item_loader.add_value("landlord_phone", "028 9066 5167")
        item_loader.add_value("landlord_email", "gavindoherty@quickletni.co.uk")

        yield item_loader.load_item()