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
    name = 'bramleys_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Bramleys_PySpider_united_kingdom"
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.bramleys.com/results-list.php?s=lettings&location=&Search=Search&minRent=0&maxRent=9999999&minPrice=0&maxPrice=9999999&minCommPrice=0&maxCommPrice=9999999&bedrooms=0&proptypeSales=all&bedrooms=0&proptypeLets=Apartment&saletype=&proptypeComms=all&propAreaMin=0&propAreaMax=999999999",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.bramleys.com/results-list.php?s=lettings&location=&Search=Search&minRent=0&maxRent=9999999&minPrice=0&maxPrice=9999999&minCommPrice=0&maxCommPrice=9999999&bedrooms=0&proptypeSales=Apartment&bedrooms=0&proptypeLets=Bungalow&saletype=&proptypeComms=all&propAreaMin=0&propAreaMax=999999999",
                    "https://www.bramleys.com/results-list.php?s=lettings&location=&Search=Search&minRent=0&maxRent=9999999&minPrice=0&maxPrice=9999999&minCommPrice=0&maxCommPrice=9999999&bedrooms=0&proptypeSales=Bungalow&bedrooms=0&proptypeLets=House&saletype=&proptypeComms=all&propAreaMin=0&propAreaMax=999999999",
                    
                    ],
                "property_type": "house"
            },
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
        for item in response.xpath("//div[contains(@class,'list-result-container')]"):
            follow_url = response.urljoin(item.xpath(".//h2/a/@href").get())
            status = item.xpath("./a/img/@alt").get()
            if status and ("agreed" in status.lower() or "under" in status.lower()):
                continue
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={"property_type": response.meta.get('property_type')}
            )
        
        next_page = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type": response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.url.split("&id=")[1].split("&")[0])
        
        title = response.xpath("//div[@class='col-md-12']/h2/text()").get()
        item_loader.add_value("title", title)
        if title:
            room_count = title.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-2])
            item_loader.add_value("zipcode", address.split(",")[-1])
        
        rent = response.xpath("//p[@class='property-price']/text()[contains(.,'Price')]").get()
        if rent:
            price = rent.split("£")[1].strip().replace(",","")
            if "pw" in price:
                price = int(price.replace("pw",""))*4
            else:
                price = price.replace("pcm","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")

        deposit = response.xpath("//p//text()[contains(.,'BOND') or contains(.,'Bond')]").get()
        if deposit:
            dep = deposit.lower().split("bonds")[-1].split("bond")[-1].replace(":","").strip().split(" ")[0].strip(".").replace("£","")
            item_loader.add_value("deposit", int(float(dep)))
        else:
            deposit = "".join(response.xpath("//h2[contains(.,'About')]/../p//text()[contains(.,'Deposit')]").getall())
            if deposit:
                deposit = deposit.lower().split("deposit")[-1].strip().split("£")[1].strip(".").split(".")[0]
                item_loader.add_value("deposit", int(float(deposit)))
        
        floor = response.xpath("//li[contains(.,'FLOOR')]//text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(" ")[0])
        
        parking = response.xpath("//li[contains(.,'GARAGE') or contains(.,'PARKING')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        elevator = response.xpath("//li[contains(.,'LIFT')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        washing_machine = response.xpath("//li[contains(.,'WASHING MACHINE')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        terrace = response.xpath("//p[@class='property-type']/text()[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        description = " ".join(response.xpath("//h2[contains(.,'About')]/../p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [x for x in response.xpath("//div[contains(@class,'slideshow')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = [x for x in response.xpath("//img[@alt='Floorplan']/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "Bramleys")
        item_loader.add_value("landlord_phone", "01484 530361")
        item_loader.add_value("landlord_email", "info@bramleys1.co.uk")
        
        yield item_loader.load_item()