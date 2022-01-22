# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from word2number import w2n
import dateparser
from datetime import datetime


class MySpider(Spider):
    name = 'wonderlease_co_uk' 
    execution_type='testing' 
    country='united_kingdom'
    locale='en'
    def start_requests(self): 
        start_urls = [
            {"url": "https://www.wonderlease.co.uk/results?searchurl=%2frental-property-search&market=1&ccode=UK&pricetype=3&proptype=Flat&statustype=4&offset=0", "property_type": "apartment"},
            {"url": "https://www.wonderlease.co.uk/results?searchurl=%2frental-property-search&market=1&ccode=UK&pricetype=3&proptype=Apartment&statustype=4&offset=0", "property_type": "apartment"},
            {"url": "https://www.wonderlease.co.uk/results?searchurl=%2frental-property-search&market=1&ccode=UK&pricetype=3&proptype=Studio&statustype=4&offset=0", "property_type": "studio"},
	        {"url": "https://www.wonderlease.co.uk/results?searchurl=%2frental-property-search&market=1&ccode=UK&pricetype=3&proptype=House&statustype=4&offset=0", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for item in response.xpath("//div[contains(@class,'results-container')]/div[@class='results-list-item']//div[@class='photoLabel']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

        pagination = response.xpath("//ul[@class='pagination']/li/a[contains(.,'Next')]/@href").get()
        if pagination:
            yield Request(response.urljoin(pagination), callback=self.parse, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        externalid=response.url
        if externalid:
            externalid=externalid.split("/")[-2]
            item_loader.add_value("external_id",externalid)
        item_loader.add_value("external_source","Wonderlease_Co_PySpider_"+ self.country)

        
        title = " ".join(response.xpath("//h1/a//text()").extract())
        item_loader.add_value("title", title)
        
        address = "".join(response.xpath("//h1/a/text()").getall())
        if address:
            address = address.replace("-","").strip()
            item_loader.add_value("address", address )
            zipcode = address.split(",")[-1].strip()
            city = address.split(zipcode)[0].strip().strip(",").split(",")[-1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//span[@class='priceask']/text()").get()
        if rent:
            price = rent.split(" ")[0].split("£")[1].replace(",","").strip()
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//div[@class='bullets-li']/p[contains(.,'Bedroom')]/text()").get()
        if room_count:
            room_count = room_count.split("Bedroom")[0].lower().replace("double","").strip().split(" ")[-1]
            if "/" in room_count:
                room_count = room_count.split("/")[-1]
            
            item_loader.add_value("room_count", w2n.word_to_num(room_count.strip()))
        
        desc = "".join(response.xpath("//div[@class='details-information']/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        if "*Available" in desc or "* Available" in desc:
            available_date = desc.split("Available")[1].split("*")[0].strip()
            if "now" in available_date.lower():
                available_date = datetime.now().strftime("%Y-%m-%d")
                item_loader.add_value("available_date", available_date)
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        if "floor" in desc:
            floor = desc.split("floor")[0].replace(",","").strip().split(" ")[-1]
            if "first" in floor or "second" in floor:
                item_loader.add_value("floor", floor)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("LatLng(")[1].split(",")[0]
            longitude = latitude_longitude.split("LatLng(")[1].split(",")[1].split(")")[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        images = [ x for x in response.xpath("//div[@class='sp-thumbnails']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        furnished = response.xpath("//div[@class='bullets-li']/p[contains(.,'Furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        balcony = response.xpath("//div[@class='bullets-li']/p[contains(.,'Balcon') or contains(.,'balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div[@class='bullets-li']/p[contains(.,'terrace') or contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//div[@class='bullets-li']/p[contains(.,'parking') or contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        item_loader.add_value("landlord_name", "WONDERLEASE")
        item_loader.add_value("landlord_phone", "020 8509 3000")
        item_loader.add_value("landlord_email", "info@wonderlease.co.uk")
        

        yield item_loader.load_item()