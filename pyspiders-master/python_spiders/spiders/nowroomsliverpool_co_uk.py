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
    name = 'nowroomsliverpool_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://nowroomsliverpool.co.uk/rooms/",
                ],
                "property_type": "room"
            }
           
        ]  # LEVEL 1
         
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for url in response.xpath("//div[@class='elementor-widget-container']//p/a/@href").getall():
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
 
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Nowroomsliverpool_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("/")[-1])

        title = " ".join(response.xpath("//div[contains(@id,'listing_heading')]//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//li[contains(@class,'key-features__feature')][2]//text()").get()
        if address:
            address = address.strip()
            if " " in address:
                address = address.split(" ")[0]
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        zipcode=response.xpath("//li[contains(@class,'key-features__feature')][3]//text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)

        rent = response.xpath("//strong[contains(@class,'price')]//text()").get()
        if rent:
            if "pw" in rent.lower():
                rent = rent.replace("£","").strip().split(" ")[0]
                rent = int(rent)*4
            else:
                rent = rent.replace("£","").strip().split(" ")[0]
            item_loader.add_value("rent", rent)
        else:
            rent = response.xpath("//section[contains(@class,'price')]//h3/text()").get()
            if rent:
                rent = rent.replace("£","").strip().split(" ")[0]
                item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        rentcheck = response.xpath("//strong[contains(@class,'price')]/following-sibling::small/text()").get()
        if rentcheck and "now let" in rentcheck.lower(): 
            return

        deposit = response.xpath("//dt[contains(.,'Deposit')]//following-sibling::dd[1]//text()").get()
        if deposit:
            deposit = deposit.replace("£","").strip().split(".")[0]
            item_loader.add_value("deposit", deposit)
        if not deposit:
            deposit1 = response.xpath("//dt[contains(.,'deposit')]//following-sibling::dd[1]//text()").get()
            if deposit1:
                deposit1 = deposit1.replace("£","").strip().split(".")[0]
                item_loader.add_value("deposit", deposit1)

        desc = " ".join(response.xpath("//p[contains(@class,'detaildesc')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        item_loader.add_value("room_count", "1")
        
        images = [x for x in response.xpath("//div[contains(@class,'photos')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//dt[contains(.,'Available')]//following-sibling::dd[1]//text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//dt[contains(.,'Parking') or contains(.,'Garage')]//following-sibling::dd[1]//text()[contains(.,'Yes')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//dt[contains(.,'Balcony')]//following-sibling::dd[1]//text()[contains(.,'Yes')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//dt[contains(.,'terrace')]//following-sibling::dd[1]//text()[contains(.,'Yes')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//dt[contains(.,'Furnishings')]//following-sibling::dd//text()[contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        latitude_longitude = response.xpath("//script[contains(.,'latitude')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude: "')[1].split('"')[0]
            longitude = latitude_longitude.split('longitude: "')[1].split('"')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Spare Room")
        item_loader.add_value("landlord_phone", "0161 768 1162")
        item_loader.add_value("landlord_email", "customerservices@spareroom.co.uk")
        
        yield item_loader.load_item()