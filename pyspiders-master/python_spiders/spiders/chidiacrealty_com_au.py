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
    name = 'chidiacrealty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://chidiacrealty.com.au/properties-for-lease?ac=&min=0&max=999999999&orderby=&type%5B%5D=apt&searchtype=2&map=&view=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://chidiacrealty.com.au/properties-for-lease?ac=&min=0&max=999999999&orderby=&type%5B%5D=dup&type%5B%5D=hou&type%5B%5D=tow&searchtype=2&map=&view=",
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
        for item in response.xpath("//div[contains(@class,'propertyTile ')]/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[contains(@class,'sd next')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Chidiacrealty_Com_PySpider_australia")
        item_loader.add_value("external_id", response.url.split("au/")[1].split("/")[0])

        title = response.xpath("//title//text()").get()
        item_loader.add_value("title", title)

        address = "".join(response.xpath("//h1//text()").getall())
        item_loader.add_value("address", address)

        city = response.xpath("//span[contains(@itemprop,'addressLocality')]//text()").get()
        item_loader.add_value("city", city)

        desc = "".join(response.xpath("//div[contains(@class,'contentRegion')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "Pets allowed:" in desc:
            item_loader.add_value("pets_allowed", True)
        
        if "lift" in desc:
            item_loader.add_value("elevator", True)
        
        rent = response.xpath("//span[contains(@class,'caption')][contains(.,'For Lease')]//following-sibling::span/text()[contains(.,'$')]").get()
        if rent:
            rent = rent.split("$")[-1].split(" ")[0].split("-")[-1]
            item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "AUD")

        from datetime import datetime
        import dateparser
        available_date = response.xpath("//span[contains(@class,'caption')][contains(.,'Available')]//following-sibling::span/text()").get()
        if "now" in available_date.lower():
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        else:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        room_count = response.xpath("//i[contains(@class,'bed')]/parent::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//i[contains(@class,'bath')]/parent::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        parking = response.xpath("//i[contains(@class,'car')]/parent::span/text()")
        if parking:
            item_loader.add_value("parking", True)
        
        images = [x for x in response.xpath("//link[contains(@itemprop,'url')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(.,'geo')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude": "')[1].split('"')[0]
            longitude = latitude_longitude.split('longitude": "')[1].split('"')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        landlord_name = response.xpath("//div[contains(@class,'agentTile-name')]//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        
        landlord_phone = response.xpath("//li[contains(@class,'agentTile-contact-mobile')]//span//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        
        landlord_email = response.xpath("//li[contains(@class,'agentTile-contact-email')]//text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", "rentals@devinere.com.au")
    
        yield item_loader.load_item()