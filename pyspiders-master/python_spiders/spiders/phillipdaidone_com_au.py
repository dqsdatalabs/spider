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
    name = 'phillipdaidone_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.phillipdaidone.com.au/renting/properties-for-rent/?property_type%5B%5D=Apartment&property_type%5B%5D=Unit&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.phillipdaidone.com.au/renting/properties-for-rent/?property_type%5B%5D=House&property_type%5B%5D=Townhouse&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
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
        for item in response.xpath("//div[contains(@class,'listing-item')]/div/a"):
            status = item.xpath(".//div[@class='sticker']/text()").get()
            if status and "leased" in status.lower():
                continue
            follow_url = item.xpath("./@href").get()
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
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

        rented = "".join(response.xpath("//div[@class='suburb-price']/text()[contains(.,'TAKEN')]").extract())
        if rented:return

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Phillipdaidone_Com_PySpider_australia")

        external_id = response.xpath("//li[contains(.,'ID')]//div//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//title//text()").get()
        item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'address')]//text()").get()
        if address:
            city = address.split(",")[-1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("address", address.strip())

        rent = response.xpath("//div[contains(@class,'price')]//text()").get()
        if rent and "$" in rent:
            price= rent.split("$")[1].strip().split(" ")[0]
            item_loader.add_value("rent", int(float(price))*4)
        item_loader.add_value("currency", "AUD")

        deposit = response.xpath("//li[contains(.,'Bond')]//div//text()").get()
        if deposit:
            deposit = deposit.split("$")[1].replace(",","")
            item_loader.add_value("deposit", deposit)
        
        room_count = response.xpath("//li[contains(.,'Bed')]//div//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room = response.xpath("//li[contains(.,'Type')]//div//text()").get()
            if room and "studio" in room.lower():
                item_loader.add_value("room_count", "1")
        
        bathroom_count = response.xpath("//li[contains(.,'Bath')]//div//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        from datetime import datetime
        import dateparser
        available_date = response.xpath("//li[contains(.,'Available')]//div//text()").get()
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        desc = " ".join(response.xpath("//div[contains(@class,'detail-description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        if desc and "floor " in desc:
            floor = desc.split("floor ")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor.replace("-","").upper())
        
        images = [x for x in response.xpath("//div[contains(@class,'slide')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'L.marker')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0]
            longitude = latitude_longitude.split('L.marker([')[1].split(',')[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        landlord_name = response.xpath("//div[contains(@class,'agent-detail')]//strong//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        landlord_email = "".join(response.xpath("//div[contains(@class,'agent-detail')]//p[contains(@class,'email')]//text()").getall())
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.strip())
        
        item_loader.add_value("landlord_phone", "02 9643 1188")
    
        yield item_loader.load_item()