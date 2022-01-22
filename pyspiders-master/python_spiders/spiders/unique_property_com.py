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
    name = 'unique_property_com'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.unique-property.com/renting/properties-for-lease/?property_type%5B%5D=Apartment&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.unique-property.com/renting/properties-for-lease/?property_type%5B%5D=House&property_type%5B%5D=Unit&property_type%5B%5D=Villa&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.unique-property.com/renting/properties-for-lease/?property_type%5B%5D=Studio&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'container')]/a[contains(@href,'lease')]"):
            status = item.xpath(".//div[@class='sticker']/text()").get()
            if status and ("leased" in status.lower()):
                continue
            follow_url = item.xpath("./@href").get()
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
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
        item_loader.add_value("external_source", "Unique_Property_PySpider_australia")

        title = response.xpath("//title//text()").get()
        item_loader.add_value("title", title)

        desc = "".join(response.xpath("//div[contains(@class,'detail-description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        if "floor" in desc.lower():
            floor = desc.lower().split("floor")[0].strip().split(" ")[-1]
            not_list = ["ti","below","location","float", "hard", "kitchen"]
            status=True
            for i in not_list:
                if i in floor:
                    status = False
            if status:
                item_loader.add_value("floor", floor)
        
        images = [x for x in response.xpath("//div[contains(@class,'item-image')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)


        rent = response.xpath("//label[contains(.,'Price')]/following-sibling::div//text()[contains(.,'$')]").get()
        if rent:
            rent = rent.split("$")[1]
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency", "USD")

        deposit = response.xpath("//label[contains(.,'Bond')]/following-sibling::div//text()").get()
        if deposit:
            deposit = deposit.split("$")[1].strip().replace(",","")
            item_loader.add_value("deposit", deposit)

        external_id = response.xpath("//label[contains(.,'ID')]/following-sibling::div//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        address = response.xpath("//label[contains(.,'Location')]/following-sibling::div//text()").get()
        item_loader.add_value("address", address)

        room_count = response.xpath("//label[contains(.,'Bed')]/following-sibling::div//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//label[contains(.,'Bath')]/following-sibling::div//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        parking = response.xpath("//label[contains(.,'Parking')]/following-sibling::div//text()").get()
        car_spaces = response.xpath("//label[contains(.,'Car')]/following-sibling::div//text()").get()
        if parking or car_spaces:
            item_loader.add_value("parking", True)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//label[contains(.,'Available')]/following-sibling::div//text()").get())
        if "now" in available_date.lower():
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        elif available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        balcony = response.xpath("//div[contains(@class,'detail-feature')]//li[contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div[contains(@class,'detail-feature')]//li[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        elevator = response.xpath("//div[contains(@class,'detail-feature')]//li[contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        swimming_pool = response.xpath("//div[contains(@class,'detail-feature')]//li[contains(.,'Pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        latitude_longitude = response.xpath("//script[contains(.,'L.marker')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0]
            longitude = latitude_longitude.split('L.marker([')[1].split(',')[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        landlord_name = response.xpath("//div[contains(@class,'agent-name')]//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        
        landlord_phone = response.xpath("//div[contains(@class,'agent-detail')]//p[3]//a//@href[contains(.,'tel')]").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.split(":")[1])

        yield item_loader.load_item()