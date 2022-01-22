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
    name = 'hoogveste_nl'
    start_urls = ['https://www.hoogveste.nl/aanbod/woningaanbod/huur/aantal-80/'] 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1
    external_source = "Hoogveste_PySpider_netherlands_nl"

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//li[contains(@class,'al2woning')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//span[contains(@class,'next-page')]/a/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        #{'Appartement': 'OK', 'OverigOG': 'OK', 'Woonhuis': 'OK', 'Bouwgrond': 'OK'}
        property_type = response.xpath("//span[.='Soort object']/parent::span/span[2]/text()").get()
        if property_type and "Appartement" in property_type:
            item_loader.add_value("property_type", "apartment")
        elif property_type and "Woonhuis" in property_type:
            item_loader.add_value("property_type", "house")
        else:
            return
        rented = response.xpath("//span[.='Status']/parent::span/span[2]/text()").get()
        if "Verhuurd" in rented:
            return
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url)

        address = response.xpath("//h1[@class='street-address']/text()").get()
        zipcode = response.xpath("//div[@class='adr addressInfo notranslate']//span[@class='postal-code']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        city = response.xpath("//div[@class='adr addressInfo notranslate']//span[@class='locality']/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        if address:
            if city:
                address = address.strip() + ', ' + city.strip()
            item_loader.add_value("address", address)         
        
        
        square_meters = response.xpath("//span[.='Woonoppervlakte']/parent::span/span[2]/text()").get()
        if square_meters:
            square_meters = square_meters.strip().split(' ')[0]
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//span[.='Aantal slaapkamers']/parent::span/span[2]/text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)
        

        price = response.xpath("//span[.='Huurprijs']/parent::span/span[2]/text()").get()
        if price: 
            # price = price.strip().split(',')[0].split(' ')[1]
            item_loader.add_value("rent_string", price)
            # item_loader.add_value("currency", "EUR")
        deposit = response.xpath("//span[.='Waarborgsom']/parent::span/span[2]/text()").get()
        if deposit: 
            deposit = deposit.split(',')[0].split('€')[1].strip()
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//span[.='Servicekosten']/parent::span/span[2]/text()").get()
        if utilities: 
            utilities = utilities.split(',')[0].split('€')[1].strip()
            item_loader.add_value("utilities", utilities)
        
        description ="".join(response.xpath("//div[@id='Omschrijving']/text()").getall())
        if description:
            item_loader.add_value("description", description)

        images = [x for x in response.xpath("//div[@class='ogFotos']/div//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
       
        floor = response.xpath("//span[.='Aantal woonlagen']/parent::span/span[2]/text()").get()
        if floor:
            if len(floor.split(' ')) > 1:
                floor = floor.split(' ')[0].strip()
            else:
                floor = floor.strip()
            item_loader.add_value("floor", floor)

        balcony = response.xpath("//span[.='Balkon']/parent::span/span[2]/text()").get()
        if balcony:
            if balcony.strip().lower() == 'ja':
                balcony = True
            else:
                balcony = False
            item_loader.add_value("balcony", balcony)
        elevator = response.xpath("//span[.='Voorzieningen']/parent::span/span[2]/text()[contains(.,'lift') or contains(.,'Lift') ]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        parking = response.xpath("//span[.='Parkeerfaciliteiten']/parent::span/span[2]/text()").get()
        if parking:
            if "geen" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        furnished = response.xpath("//span[.='Specificatie']/parent::span/span[2]/text()").get()
        if furnished:
            if "gemeubileerd" in furnished.lower():
                item_loader.add_value("furnished", True)
        item_loader.add_value("landlord_name", "Hoogveste")
        item_loader.add_value("landlord_phone", "0184-210000")
        item_loader.add_value("landlord_email", "info@hoogveste.nl")
        yield item_loader.load_item()