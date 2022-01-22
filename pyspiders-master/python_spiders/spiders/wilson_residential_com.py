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
    name = 'wilson_residential_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.wilson-residential.com/search?sta=toLet&st=rent&pt=residential&pt=commercial&pt=land&pt=agricultural&stygrp=3", 
                "property_type": "apartment"
            },
	        {
                "url": "https://www.wilson-residential.com/search?sta=toLet&st=rent&pt=residential&pt=commercial&pt=land&pt=agricultural&stygrp=2&stygrp=10&stygrp=8&stygrp=9&stygrp=6", 
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//li[contains(@class,'itm')]//div[contains(@class,'itm-details')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
                

        item_loader.add_value("external_source", "Wilson_Residential_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("/")[-1])

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//th[contains(.,'Address')]//following-sibling::td//text()").get()
        if address:
            city = address.split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)

        zipcode = " ".join(response.xpath("//span[contains(@class,'outcode')]//text() | //span[contains(@class,'incode')]//text()").getall())
        if zipcode:
            item_loader.add_value("zipcode", zipcode)

        rent = "".join(response.xpath("//th[contains(.,'Rent')]//following-sibling::td/text()").getall())
        if rent:
            rent = rent.strip().replace("£","").replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        deposit = response.xpath("//th[contains(.,'Deposit')]//following-sibling::td//text()").get()
        if deposit:
            deposit = deposit.split("£")[1].split(".")[0].replace(",","")
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//section[contains(@class,'listing-additional-info')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        else:
            desc = " ".join(response.xpath("//section[contains(@class,'listing-additional-info')]/text()").getall())
            if desc:
                desc = re.sub('\s{2,}', ' ', desc.strip())
                item_loader.add_value("description", desc)
        
        room_count = response.xpath("//th[contains(.,'Bedroom')]//following-sibling::td//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//th[contains(.,'Bathroom')]//following-sibling::td//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'slideshow-thumbs')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//th[contains(.,'Available')]//following-sibling::td//text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        furnished = response.xpath("//th[contains(.,'Furnished')]//following-sibling::td//text()[not(contains(.,'Unfurnished'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        energy_label = "".join(response.xpath("//th[contains(.,'EPC')]//following-sibling::td//text()").getall())
        if energy_label:
            energy_label = energy_label.split("/")[0].strip()
            item_loader.add_value("energy_label", energy_label)

        latitude = response.xpath("//meta[contains(@property,'latitude')]//@content").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        longitude = response.xpath("//meta[contains(@property,'longitude')]//@content").get()
        if longitude:  
            item_loader.add_value("longitude", longitude)

        parking = response.xpath("//li[contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        item_loader.add_value("landlord_name", "Wilson Residential")
        item_loader.add_value("landlord_phone", "028 4062 4400")
        item_loader.add_value("landlord_email", "info@wilson-residential.com")

        yield item_loader.load_item()