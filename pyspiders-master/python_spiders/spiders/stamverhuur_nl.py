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
from datetime import datetime
import re

class MySpider(Spider):
    name = 'stamverhuur_nl'
    execution_type='testing'
    country='netherlands'
    locale='nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.stamverhuur.nl/woningaanbod/huur/type-appartement?moveunavailablelistingstothebottom=true&orderby=8",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.stamverhuur.nl/woningaanbod/huur/type-woonhuis?moveunavailablelistingstothebottom=true&orderby=8",
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

        for item in response.xpath("//a[@class='img-container']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
        next_page = response.xpath("//a[contains(@class,'next-page')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta['property_type']}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Stamverhuur_PySpider_netherlands")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = response.xpath("//h1[@class='obj_address']/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = " ".join(address.split(",")[-1].strip().split(" ")[:2])
            city = address.split(",")[-1].split(zipcode)[-1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
             
        rent = response.xpath("normalize-space(//div[@class='object_price']/text())").get()
        if rent:
            price = rent.split(",")[0].split("€")[1].strip().replace(".","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        else:
            rent = response.xpath("//tr/td[contains(.,'Prijs')]/following-sibling::td/text()").get()
            if rent:
                price = rent.split(",")[0].split("€")[1].strip().replace(".","")
                item_loader.add_value("rent", price)
                item_loader.add_value("currency", "EUR")

        
        square_meters = response.xpath("//tr/td[contains(.,'Gebruiksoppervlakte')]/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(" ")[0])
        
        ext_id = response.xpath("//tr/td[contains(.,'eferentienummer')]/following-sibling::td/text()").get()
        if ext_id:
            item_loader.add_value("external_id", ext_id.strip())
        
        room_count = response.xpath("//tr/td[contains(.,'Aantal kamer')]/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("(")[0])
        
        deposit = response.xpath("normalize-space(//tr/td[contains(.,'Borg')]/following-sibling::td/text())").get()
        if deposit:
            deposit = deposit.split(",")[0].split("€")[1].strip().replace(".", "")
            item_loader.add_value("deposit", deposit)
        
        available_date = response.xpath("//tr/td[contains(.,'Aanvaarding')]/following-sibling::td/text()").get()
        if available_date:
            if "direct" in available_date.lower():
                available_date = datetime.now().strftime("%Y-%m-%d")
                item_loader.add_value("available_date", available_date)
            elif "Per" in available_date:
                available_date = available_date.split(" ")
                available_date = available_date[-3]+" "+available_date[-2]+" "+available_date[-1]
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                    
        floor = response.xpath("//tr/td[contains(.,'Woonlaag')]/following-sibling::td/text()").get()
        if floor:
            item_loader.add_value("floor", floor.replace("e woonlaag",""))
        
        latitude_longitude = response.xpath("//script[contains(.,'center')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("center: [")[1].split(",")[0]
            longitude = latitude_longitude.split("center: [")[1].split(",")[1].split("]")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        desc = "".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[@id='object-photos']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            
        item_loader.add_value("landlord_name", "Stam Vastgoed")
        item_loader.add_value("landlord_phone", "31 35 201 8411")
        item_loader.add_value("landlord_email", "info@stamverhuur.nl")
        
        yield item_loader.load_item()