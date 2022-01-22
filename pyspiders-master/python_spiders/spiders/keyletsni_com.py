# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'keyletsni_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.keyletsni.com/search?sta=toLet&st=rent&pt=residential&stygrp=3&stygrp=6",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.keyletsni.com/search?sta=toLet&st=rent&pt=residential&stygrp=2&stygrp=10&stygrp=8&stygrp=9",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.keyletsni.com/search?sta=toLet&st=rent&pt=residential&stygrp=7",
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
        for item in response.xpath("//div[contains(@class,'PropBox-content ')]/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[contains(@class,'Paging-next')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={
                    "property_type":response.meta["property_type"],
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Keyletsni_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_value("external_id", item_loader.get_collected_values("external_link")[0].split('/')[-1].strip())

        rent = "".join(response.xpath("//tr[contains(@class,'KeyInfo-price')]/td/span[@class='price-text']/text()").extract())
        if rent:
            price = rent.replace(",","")
            item_loader.add_value("rent_string", price.strip())

        address = "".join(response.xpath("//tr[contains(@class,'KeyInfo-address')]/td//text()").extract())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))

        item_loader.add_xpath("latitude", "//meta[@property='og:latitude']/@content")
        item_loader.add_xpath("longitude", "//meta[@property='og:longitude']/@content")


        item_loader.add_xpath("room_count", "normalize-space(//tr[contains(@class,'KeyInfo-bedroom')]/td/text())")
        item_loader.add_xpath("bathroom_count", "normalize-space(//tr[contains(@class,'KeyInfo-bathroom')]/td/text())")

        city = response.xpath("//h1//span[@class='Address-addressLocation']/span/text()").extract_first()
        if city:
            item_loader.add_value("city", city.replace(",","").strip())

        zipcode = " ".join(response.xpath("//h1//span[@class='Address-addressPostcode']/span/text()").extract())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        deposit = "".join(response.xpath("//tr[contains(@class,'KeyInfo-deposit')]/td/text()").extract())
        if deposit:          
            item_loader.add_value("deposit", deposit.strip())

        floor = "".join(response.xpath("//tr[contains(@class,'KeyInfo-style')]/td/text()[contains(.,'Floor')]").extract())
        if floor:        
            item_loader.add_value("floor", floor.split("Floor")[0].strip())

        description = " ".join(response.xpath("//div[@class='ListingDescr-text']//text()").getall())  
        if description:
            item_loader.add_value("description", description.strip())

        available_date="".join(response.xpath("//tr[contains(@class,'KeyInfo-row KeyInfo-availablefrom')]/td/text()").getall())

        if available_date:
            date2 =  available_date.strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        images = [x for x in response.xpath("//div[@class='Slideshow-thumbs']/a/@href[not(contains(.,'=video') or contains(.,'=tour'))]").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished = response.xpath("normalize-space(//tr[contains(@class,'KeyInfo-furnished')]/td/text())").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished",False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished",True)

        item_loader.add_value("landlord_phone", "028 9002 0549")
        item_loader.add_value("landlord_name", "Key Lets NI")
        item_loader.add_value("landlord_email", "keyletproperty@gmail.com")

        yield item_loader.load_item()