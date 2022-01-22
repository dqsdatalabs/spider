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
    name = 'dck_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.dck.com.au/renting/properties-for-rent/?property_type%5B%5D=Unit&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=", 
                "property_type": "apartment"
            },
	        {
                "url": "https://www.dck.com.au/renting/properties-for-rent/?property_type%5B%5D=House&property_type%5B%5D=Townhouse&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                 "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'listing-item')]/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page: yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Dck_Com_PySpider_australia")

        external_id = response.xpath("//p[contains(@class,'property-id')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//h4[contains(@class,'property-address')]//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//h4[contains(@class,'property-address')]//text()").get()
        if address:
            if "street" in address.lower():
                city = address.split("VIC")[0].split("Street")[-1]
            else:
                city = address.strip().split(" ")[-2]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
        
        square_meters = response.xpath("//label[contains(@class,'detail-label')][contains(.,'Building Size')]//following-sibling::div//text()").get()
        if square_meters:
            square_meters = square_meters.split("sqm")[0].strip().split(".")[0]
            item_loader.add_value("square_meters", square_meters)

        rent = "".join(response.xpath("//div[contains(@class,'price')]//text()").getall())
        if rent:
            if "per week" in rent:
                rent =  rent.split(".")[0].replace("$","").replace("per week","").strip()
            else:
                rent = rent.split("$")[1].strip().split(" ")[0].split(".")[0]
            item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "AUD")

        deposit = response.xpath("//label[contains(@class,'detail-label')][contains(.,'Bond')]//following-sibling::div//text()").get()
        if deposit:
            deposit = deposit.strip().replace(",","").replace("$","")
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[contains(@class,'detail-description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//i[contains(@class,'bed')]//parent::li//span//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//i[contains(@class,'bath')]//parent::li//span//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'main-carousel')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//label[contains(@class,'detail-label')][contains(.,'Available')]//following-sibling::div//text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//i[contains(@class,'car')]//parent::li//span//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//label[contains(@class,'detail-label')][contains(.,'Garage')]//following-sibling::div//text()").get()
            if parking:
                item_loader.add_value("parking", True)
            
        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        latitude_longitude = response.xpath("//script[contains(.,'L.marker')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0]
            longitude = latitude_longitude.split('L.marker([')[1].split(",")[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        landlord_name = response.xpath("//div[contains(@class,'agent-detail')]//strong//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        landlord_email = "".join(response.xpath("//div[contains(@class,'agent-detail')]//p[contains(@class,'email')]//text()").getall())
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        
        landlord_phone = "".join(response.xpath("//div[contains(@class,'agent-detail')]//p[contains(@class,'phone')]//text()").getall())
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()