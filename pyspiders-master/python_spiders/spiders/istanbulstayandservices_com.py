# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider, item
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'istanbulstayandservices_com' 
    execution_type = 'testing'
    country = 'turkey'
    locale ='tr'
    external_source="IstanbulstayServices_PySpider_turkey"
    start_urls = ['https://istanbulstayandservices.com/long-term/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        # url = "https://istanbulstayandservices.com/properties/very-special-sea-view-furnished-flat-for-rent/"
        # yield Request(url, callback=self.populate_item)

        for item in response.xpath("//div[contains(@class,'isotope-item all')]//a[@class='infobox-image']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//li[span[contains(.,'Type')]]/text()").get()
        if property_type and ("apartment" in property_type.lower() or "flat" in property_type.lower()):
            item_loader.add_value("property_type", "apartment")
        else: return
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//h1/text()").get()
        item_loader.add_value("title", title)
        
        external_id = response.xpath("substring-after(//link[@rel='shortlink']/@href,'=')").get()
        item_loader.add_value("external_id", external_id)
        
        
        street = " ".join(response.xpath("//div[contains(@class,'pull-left')]/div[@class='property-row-address']/text()").getall())
        district = response.xpath("//li[span[contains(.,'Location')]]/a/text()").get()

        item_loader.add_value("address", f"{street} {district} Istanbul".strip())
        item_loader.add_value("city", "Istanbul")
        
        zipcode = response.xpath("//li[span[contains(.,'Zip')]]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        square_meters = response.xpath("//li[span[contains(.,'area')]]/text()").get()
        if square_meters:
            if "sqft" in square_meters:
                sqm = square_meters.replace("\t","").split("sq")[0].replace(",",".").strip()
                square_meters = str(int(float(sqm)* 0.09290304))
            square_meters = square_meters.replace("\t","").split("sq")[0].replace(",",".").strip()
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//li[span[contains(.,'Bed')]]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//span[text()='Rooms:']/following-sibling::text()").get()
            if room_count:
                item_loader.add_value("room_count",room_count.strip())
        
        bathroom_count = response.xpath("//li[span[contains(.,'Bath')]]/text()").get()
        if bathroom_count:
            if "." in bathroom_count: bathroom_count = bathroom_count.split(".")[0]
            item_loader.add_value("bathroom_count", bathroom_count.strip())
            
        rent = "".join(response.xpath("//header[contains(@class,'no-margin')]//div[contains(@class,'price')]//text()[not(contains(.,'Per'))]").getall())
        if rent:
            rent = rent.replace(",","").strip()
            if "EUR" in rent.strip():
                item_loader.add_value("rent", rent)
                # item_loader.add_value("currency", "EUR")

            elif "USD" in rent.strip():
                item_loader.add_value("rent", rent)
                # item_loader.add_value("currency", "USD")

            else:
                item_loader.add_value("rent_string", rent)

        else:
            
            rent = "".join(response.xpath("//div[@class='container']/div[@class='header-right pull-right']//div[@class='property-box-price text-theme']//text()").getall())
            if rent:
                rent = rent.replace(",","").strip()
                if "USD" in rent:
                    # item_loader.add_value("currency", "USD")
                    item_loader.add_value("rent", rent.strip().split(" ")[0])
                else:
                    item_loader.add_value("rent_string", rent.strip())
        # item_loader.add_value("currency", "TRY")

        currency=response.xpath("//div[@class='property-box-price text-theme']//span[@class='subfix']/text()").get()
        if currency and not "TL" in currency:
            item_loader.add_value("currency",currency)
        else:
            item_loader.add_value("currency","TRY")
        
        desc = "".join(response.xpath("//div[@id='property-section-description']//p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        else:
            desc = "".join(response.xpath("//div[@id='property-section-description']/ul/li").getall())
            if desc:
                item_loader.add_value("description",desc)
        
        dishwasher = response.xpath("//li[@class='yes']/text()[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing_machine = response.xpath("//li[@class='yes']/text()[contains(.,'washer ')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        balcony = response.xpath("//li[@class='yes']/text()[contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        swimming_pool = response.xpath("//li[@class='yes']/text()[contains(.,'pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        elevator = response.xpath("//li[@class='yes']/text()[contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//li[@class='yes']/text()[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        latitude = response.xpath("//@data-latitude").get()
        if latitude:
            item_loader.add_value("latitude", latitude.strip())
        
        longitude = response.xpath("//@data-longitude").get()
        if longitude:
            item_loader.add_value("longitude", longitude.strip())
        
        images = [x for x in response.xpath("//div[contains(@class,'gallery')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        landlord_name = response.xpath("//li[span[contains(.,'name')]]/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        
        landlord_phone = response.xpath("//li[span[contains(.,'phone')]]/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
            
        item_loader.add_value("landlord_email", "billur@istanbulstayandservices.com")
        
        yield item_loader.load_item()