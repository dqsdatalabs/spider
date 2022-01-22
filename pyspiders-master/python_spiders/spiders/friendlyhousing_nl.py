# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
import dateparser
import re

class MySpider(Spider):
    name = 'friendlyhousing_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source='Friendlyhousing_PySpider_netherlands'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://friendlyhousing.nl/nl/aanbod/woonhuis",
                ],
                "property_type" : "house",
            },
            {
                "url" : [
                    "https://friendlyhousing.nl/nl/aanbod/appartement",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://friendlyhousing.nl/nl/aanbod/studio",
                ],
                "property_type" : "studio"
            },
            {
                "url" : [
                    "https://friendlyhousing.nl/nl/aanbod/kamer",
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
        for item in response.xpath("//div[@class='content']/h3/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
    
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source","Friendlyhousing_PySpider_"+ self.country)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = "".join(response.xpath("//h1//text()").getall())
        if address:
            item_loader.add_value("address", address)
        
        city = response.xpath("//div[contains(@class,'property')]//div[contains(.,'Stad')]/following-sibling::div/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        
        zipcode = response.xpath("//div[contains(@class,'property')]//div[contains(.,'Postcode')]/following-sibling::div/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        rent = "".join(response.xpath("//h3[contains(@class,'rentPrice')]/text()").getall())
        if rent:
            price = rent.split("€")[1].split(",")[0].strip().replace(".","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
            
        room_count = response.xpath("//div[contains(@class,'property')]//div[contains(.,'Aantal slaapkamers') or contains(.,'Aantal kamers')]/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        
        square_meters = response.xpath("//div[contains(@class,'property')]//div[contains(.,'Oppervlakte')]/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip().split(" ")[0])
        
        deposit = response.xpath("//div[contains(@class,'property')]//div[contains(.,'Borgsom')]/following-sibling::div/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[1].strip())
        
        utilities = response.xpath("//h3[@class='rentPrice']/span[contains(.,'€')]/text()").get()
        if utilities:
            utilities = utilities.split("€")[1].strip().split(" ")[0]
            item_loader.add_value("utilities", utilities)
        
        available_date = response.xpath("//div[contains(@class,'property')]//div[contains(.,'Aanvaardingsdatum')]/following-sibling::div/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        desc = "".join(response.xpath("//div[contains(@class,'read-more-wrap')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//a[contains(@class,'thumb magnific image')]/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        lat_lng = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if lat_lng:
            latitude = lat_lng.split("LatLng(")[1].split(",")[0]
            longitude = lat_lng.split("LatLng(")[1].split(",")[1]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        furnished = response.xpath("//div[contains(@class,'property')]//div[contains(.,'Gestoffeerd') or contains(.,'Gemeubileerd')]/following-sibling::div/i/@class[contains(.,'check')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        parking = response.xpath("//div[contains(@class,'property')]//div[contains(.,'Parkeergelegenheid')]/following-sibling::div/i/@class[contains(.,'check')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//div[contains(@class,'property')]//div[contains(.,'Balkon')]/following-sibling::div/i/@class[contains(.,'check')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
            
        elevator = response.xpath("//div[contains(@class,'property')]//div[contains(.,'Lift')]/following-sibling::div/i/@class[contains(.,'check')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        item_loader.add_value("landlord_name", "Friendly Housing")
        item_loader.add_value("landlord_phone", "31(0)40-244 44 48")
        item_loader.add_value("landlord_email", "info@friendlyhousing.nl")
        
        yield item_loader.load_item()