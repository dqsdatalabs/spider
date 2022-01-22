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
    name = 'positionproperty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Positionproperty_Com_PySpider_australia'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.positionproperty.com.au/renting/property-search?ltype=6&pype=2",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.positionproperty.com.au/renting/property-search?ltype=6&pype=1",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'results')]/div/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

        next_button = response.xpath("//a[@aria-label='Next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.url.split("-")[-1])

        title = " ".join(response.xpath("//title//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//title//text()").get()
        if address:
            address = address.split("-")[0]
            item_loader.add_value("address", address.strip())

        city = response.xpath("//meta[contains(@property,'locality')]//@content").get()
        if city:
            item_loader.add_value("city", city.strip())

        zipcode = " ".join(response.xpath("//meta[contains(@property,'region')]//@content | //meta[contains(@property,'postal-code')]//@content").getall())
        if zipcode:
            zipcode = zipcode.strip()
            item_loader.add_value("zipcode", zipcode)
        latlng=response.xpath("//script[contains(.,'locations')]/text()").get()
        if latlng:
            latlng=latlng.split("locations=")[-1].split("-")[-1].split("]")[0]
            lat=latlng.split(",")[0] 
            lng=latlng.split(",")[-1]
            if lat:
                item_loader.add_value("latitude",lat)
            if lng:
                item_loader.add_value("longitude",lng)
        square_meters = "".join(response.xpath("//p[contains(.,'Living')]//text()").getall())
        if square_meters:
            square_meters = square_meters.split("sq")[0].strip()
            if "*" in square_meters:
                square_meters = square_meters.replace("*","").strip()
            else:
                square_meters = square_meters.strip().split(" ")[-1]
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//p[contains(@class,'price')]//text()").get()
        if rent:
            rent = rent.strip().split("$")[-1].split(" ")[0].replace(")",'').strip()
            item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "AUD")

        deposit = "".join(response.xpath("//p[contains(.,'Bond')]//text()").getall())
        if deposit:
            deposit = deposit.split("Bond")[1].strip().split(" ")[0].replace(",","").replace("$","")
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[contains(@class,'desc')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//img[contains(@class,'bed')]//parent::li//strong/text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//img[contains(@class,'bath')]//parent::li//strong/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//li[contains(@class,'swipe main')]//@data-src | //li[contains(@class,'thumbs')]//@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//span[contains(@class,'view-fp picture')]//@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//p[contains(.,'Available')]//text()").getall())
        if available_date:
            available_date = available_date.split("Available")[1].split("Bond")[0].replace(",","")
            if not "now" in available_date.lower():
                if "-" in available_date:
                    available_date = available_date.split("-")[0]
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//img[contains(@class,'car')]//parent::li//strong/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//strong[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//strong[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
            
        swimming_pool = response.xpath("//strong[contains(.,'Swimming Pool')]//text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        landlord_name = "".join(response.xpath("//div[contains(@class,'staff')]//span//text()").getall())
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        item_loader.add_value("landlord_phone", "07 3325 7800")

        yield item_loader.load_item()