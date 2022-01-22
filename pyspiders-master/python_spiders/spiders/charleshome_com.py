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
import dateparser

class MySpider(Spider):
    name = 'charleshome_com'
    execution_type='testing'
    country='belgium'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.charleshome.com/brussels-apartments/?location=all&type=one-bedroom&status=all&pageid=1149",
                    "https://www.charleshome.com/brussels-apartments/?location=all&type=two-bedrooms&status=all&pageid=1149",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.charleshome.com/brussels-apartments/?location=all&type=studio&pageid=167&form=mini",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'property-item')]/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[contains(@class,'next ')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]}
            )    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Charleshome_PySpider_belgium")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
      
        title = response.xpath("//h1/span/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        address = response.xpath("//h1/span/text()").get()
        if address:
            item_loader.add_value("address", address)
        
        square_meters = response.xpath("//div[@title='Size']/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        room_count = response.xpath("//div[@title='Rooms']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        bathroom_count = response.xpath("//div[@title='Bathrooms']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        rent = response.xpath("//td[contains(.,'more')]/following-sibling::td/text()").get()
        if rent:
            price = rent.split("€")[0].strip().replace(".","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        
        desc = " ".join(response.xpath("//section[@id='property-content']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[@id='property_image_slider']//@data-image").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("LatLng(")[1].split(",")[0].strip()
            longitude = latitude_longitude.split("LatLng(")[1].split(",")[1].split(")")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        external_id = response.xpath("//div[@title='Property ID']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        available_date = response.xpath("//div[@title='Available From']/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//div[@id='additional-2']//td[contains(.,'more')]/following-sibling::td//text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].strip().replace(".",""))
        
        terrace = "".join(response.xpath("//li[contains(.,'Terrace')]/i[contains(@class,'check')]/parent::li/text()").getall())
        if terrace:
            item_loader.add_value("terrace", True)
        
        balcony = "".join(response.xpath("//li[contains(.,'Balcon')]/i[contains(@class,'check')]/parent::li/text()").getall())
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator = "".join(response.xpath("//li[contains(.,'Elevator')]/i[contains(@class,'check')]/parent::li/text()").getall())
        if elevator:
            item_loader.add_value("elevator", True)
        
        dishwasher = "".join(response.xpath("//li[contains(.,'Dishwasher')]/i[contains(@class,'check')]/parent::li/text()").getall())
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing_machine = "".join(response.xpath("//li[contains(.,'Washing Machine')]/i[contains(@class,'check')]/parent::li/text()").getall())
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        item_loader.add_value("landlord_name", "Charles Home")
        item_loader.add_value("landlord_phone", "32 (0)2 318 42 10")
        item_loader.add_value("landlord_email", "contact@charleshome.com")
        
        
        yield item_loader.load_item()