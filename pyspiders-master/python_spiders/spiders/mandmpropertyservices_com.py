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
    name = 'mandmpropertyservices_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.mandmpropertyservices.com/grid?sta=toLet&st=rent&pt=residential&stygrp=3",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.mandmpropertyservices.com/grid?sta=toLet&st=rent&pt=residential&stygrp=2&stygrp=9&stygrp=6",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.mandmpropertyservices.com/grid?sta=toLet&st=rent&pt=residential&stygrp=7",
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

        for item in response.xpath("//div[@class='list-item-content']/h2/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='paging-next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        item_loader.add_value("external_source", "Mandmpropertyservices_PySpider_united_kingdom")

        title = " ".join(response.xpath("//h1[contains(@class,'addr')]//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            item_loader.add_value("address", title)
        
        city = response.xpath("//div[contains(@class,'listing-address')]//span[contains(@class,'addr-town')]//text()").get()
        if city:
            item_loader.add_value("city",city)
        
        zipcode = "".join(response.xpath("//div[contains(@class,'listing-address')]//span[contains(@class,'addr-postcode')]//text()").getall())
        if zipcode:
            zipcode = re.sub('\s{2,}', ' ', zipcode.strip())
            item_loader.add_value("zipcode", zipcode.replace("\n"," "))
        
        rent = response.xpath("//tr[contains(.,'Rent')]//span[contains(@class,'price')]//text()[not(contains(.,'POA'))]").get()
        if rent:
            rent = rent.strip().replace(",","").split("£")[1]
            item_loader.add_value("rent", int(float(rent)))
        item_loader.add_value("currency", "GBP")
        
        deposit = response.xpath("//tr[contains(.,'Deposit')]//span[contains(@class,'price')]//text()").get()
        if deposit:
            deposit = deposit.strip().replace(",","").split("£")[1]
            item_loader.add_value("deposit", int(float(deposit)))
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//tr[contains(.,'Available')]//td//text()").getall())
        if available_date:
            available_date = available_date.split("From")[1].strip()
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            
        room_count = "".join(response.xpath("//tr[contains(.,'Bedroom')]//td//text()").getall())
        if room_count:
            room_count = room_count.split("Bedrooms")[1].strip()
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = "".join(response.xpath("//tr[contains(.,'Bathroom')]//td//text()").getall())
        if bathroom_count:
            bathroom_count = bathroom_count.split("Bathrooms")[1].strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        furnished = "".join(response.xpath("//tr[contains(.,'Furnished')]//td//text()").getall())
        if furnished and "unfurnished" not in furnished.lower():
            item_loader.add_value("furnished", True)
        
        desc = " ".join(response.xpath("//section[contains(@class,'listing-additional-info')]//text()").getall())
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

        item_loader.add_value("landlord_name", "M&M PROPERTY SERVICES")
        item_loader.add_value("landlord_phone", "028 9024 5555")
        item_loader.add_value("landlord_email", "mandmpropertyservices@gmail.com")
        
        status = "".join(response.xpath("//tr[contains(.,'Status')]//td//text()").getall())
        if status and "to let" in status.lower():
             yield item_loader.load_item()