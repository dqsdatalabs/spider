# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'bryanestates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://bryanestates.co.uk/site/go/search?sales=false&min=0&beds=0&items=12&type=2685&max=0&location=&search=&page=1&up=false&sort=onmarket",
                    "http://bryanestates.co.uk/site/go/search?sales=false&min=0&beds=0&items=12&type=2686&max=0&location=&search=&page=1&up=false&sort=onmarket",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://bryanestates.co.uk/site/go/search?sales=false&min=0&beds=0&items=12&type=2683&max=0&location=&search=&page=1&up=false&sort=onmarket",
                    "http://bryanestates.co.uk/site/go/search?sales=false&min=0&beds=0&items=12&type=2684&max=0&location=&search=&page=1&up=false&sort=onmarket",
                    "http://bryanestates.co.uk/site/go/search?sales=false&min=0&beds=0&items=12&type=2687&max=0&location=&search=&page=1&up=false&sort=onmarket",
                    "http://bryanestates.co.uk/site/go/search?sales=false&min=0&beds=0&items=12&type=2689&max=0&location=&search=&page=1&up=false&sort=onmarket",
                    "http://bryanestates.co.uk/site/go/search?sales=false&min=0&beds=0&items=12&type=2895&max=0&location=&search=&page=1&up=false&sort=onmarket",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "http://bryanestates.co.uk/site/go/search?sales=false&min=0&beds=0&items=12&type=2688&max=0&location=&search=&page=1&up=false&sort=onmarket",
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

        for item in response.xpath("//div[@id='propertiesList']/div/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Bryanestates_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("ID=")[1])
        
        title = response.xpath("//div[contains(@id,'particularsBedroom')]/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        room_count = response.xpath("//title/text()").get()
        if room_count:
            room_count = room_count.split(" ")[0]
            if "studio" in room_count.lower():
                item_loader.add_value("room_count", "1")
            elif room_count.isdigit():
                item_loader.add_value("room_count", room_count)

        address = response.xpath("//div[contains(@id,'particularsAddress')]/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip().split(",")[-2].strip())
            item_loader.add_value("zipcode", address.strip().split(",")[-1].strip())
        
        rent = "".join(response.xpath("//div[contains(@id,'particularsPrice')]/text()").getall())
        if rent:
            price = rent.strip().replace("£","")
            item_loader.add_value("rent", int(float(price))*4)
        item_loader.add_value("currency", "GBP")
        
        square_meters = response.xpath("//li[contains(.,'Sqm')]//text()").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0]
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        latitude_longitude = response.xpath("//script[contains(.,'setView([')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('setView([')[1].split(',')[0]
            longitude = latitude_longitude.split('setView([')[1].split(',')[1].split(']')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        terrace = response.xpath("//li[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        furnished = response.xpath("//li[contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        balcony = response.xpath("//li[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        floor = response.xpath("//li[contains(.,'Floor ')]//text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(" ")[0])
        
        description = " ".join(response.xpath("//div[@id='description']//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [x for x in response.xpath("//div[@class='sp-slides']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='floorplans']//@src").getall()]
        item_loader.add_value("floor_plan_images", floor_plan_images)
        
        item_loader.add_value("landlord_name", "Bryan Estates")
        item_loader.add_value("landlord_phone", "020 7998 4788")
        item_loader.add_value("landlord_email", "info@bryanestates.co.uk")
        
        yield item_loader.load_item()