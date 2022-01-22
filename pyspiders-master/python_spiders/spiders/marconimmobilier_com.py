# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim
import dateparser

class MySpider(Spider):
    name = 'marconimmobilier_com'
    start_urls = ['https://www.marcon-immobilier.com/en/rentals']  # LEVEL 1
    execution_type='testing'
    country='france'
    locale='fr'
    thousand_separator = ','
    scale_separator = '.'      
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//ul[contains(@class,'list')]/li"):
            
            follow_url = response.urljoin(item.xpath("./a[last()]/@href").get())
            prp_type = item.xpath(".//h2/text()").get()
            if "apartment" in prp_type or "Apartment" in prp_type or "studio" in prp_type:
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': 'apartment'})
            elif "house" in prp_type or "House" in prp_type or "duplex" in prp_type:
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': 'house'})
        pagination = response.xpath("//li[@class='next']/a[@rel='next']/@href").get()
        if pagination:
            url = response.urljoin(pagination)
            yield Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Marconimmobilier_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_xpath("title", "//h1/text()")

        item_loader.add_value("external_link", response.url)

        external_id = response.xpath("//li[span[.='Reference']]/text()").get()
        if external_id:
            external_id = external_id.strip()
        item_loader.add_value("external_id", external_id)

        item_loader.add_value("property_type", response.meta.get("property_type"))

        address = response.xpath("//div[@class='info']//h2/text()[last()]").extract_first()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())     

        square_meters = response.xpath("//li[text()='Area ']/span/text()").get()
        if square_meters:
            square_meters = square_meters.split('m²')[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))

        bathroom = response.xpath("//li[contains(.,'Bathroom')]/text()").extract_first()
        if bathroom :
            item_loader.add_value("bathroom_count", bathroom.split("Bathroom")[0].strip())
        else:
            bathroom = response.xpath("//li[contains(.,' Shower')]/text()").extract_first()
            if bathroom :
                item_loader.add_value("bathroom_count", bathroom.split("Shower")[0].strip())

        room_count1 = response.xpath("//ul//li[contains(.,'Bedroom')]/text()").get()
        room_count2=response.xpath("//ul/li[contains(.,'Living room')]/text() | //ul//li[contains(.,'Living-room')]/text()").get()
        if room_count1 and room_count2:
            room_count1 = room_count1.split(" bed")[0].split(" Bed")[0].split(" ")[-1]
            room_count2=room_count2.split(" Li")[0].split(" ")[-1]
            item_loader.add_value("room_count", int(room_count1)+int(room_count2)) 
        
        if room_count1 and not room_count2:
            room_count1 = room_count1.split(" bed")[0].split(" Bed")[0].split(" ")[-1]
            item_loader.add_value("room_count", int(room_count1)) 
        if room_count2 and not room_count1:
            room_count2 = room_count2.split(" Li")[0].split(" ")[-1]
            item_loader.add_value("room_count", int(room_count2)) 

        price = response.xpath("//div[@class='info']//li[contains(.,'€')]//text()").get()
        if price:
            item_loader.add_value("rent_string", price)

        deposit = response.xpath("//li[contains(.,'Deposit')]/span/text()").get()
        if deposit:
            dep = deposit.split("€")[0]
            item_loader.add_value("deposit", int(float(dep)))

        utilities = response.xpath("//li[contains(.,'Provision ')]/span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0])

        swimming = "".join(response.xpath("//li[contains(.,'pool')]/text()").extract())
        if swimming:
            item_loader.add_value("swimming_pool", True)

        parking = "".join(response.xpath("//li[contains(.,'parking')]/text()").extract())
        if parking:
            item_loader.add_value("parking", True)

        elevator = "".join(response.xpath("//li[contains(.,'Lift')]/text()").extract())
        if elevator:
            item_loader.add_value("elevator", True)
        
        images = [x for x in response.xpath("//div[@class='slider']//a/img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)

        floor = response.xpath("//li[contains(.,'Floor')]/span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        landlord_phone = response.xpath("//ul[@class='listing']//span[@class='phone']/a/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            landlord_phone = response.xpath("//ul[@class='listing']//span[@class='mobile']/a/text()").get()
            if landlord_phone:
                landlord_phone = landlord_phone.strip()
                item_loader.add_value("landlord_phone", landlord_phone)
        landlord_name = response.xpath("//ul[@class='listing']//h3/a/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "Marcon Immobilier")
        landlord_email = response.xpath("//ul[@class='listing']//span[@class='email']/a/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        

        yield item_loader.load_item()