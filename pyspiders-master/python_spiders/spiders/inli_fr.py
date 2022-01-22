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
    name = 'inli_fr'
    execution_type='testing' 
    country = 'france'
    locale = 'fr' 
    start_urls = ["https://www.inli.fr/"]

    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'real-estate-ad-list__item--cities')]/ul/li/a/@href").getall():
            f_url = response.urljoin(item)
            yield Request(f_url, callback=self.jump)

    # 1. FOLLOWING
    def jump(self, response):
        for item in response.xpath("//a[@class='thumbnail__text']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        desc = "".join(response.xpath("//h1[@class='page-bien__header__description--address']/text()").getall())
        if desc and "appartement" in desc.lower():
            item_loader.add_value("property_type", "apartment")
        elif desc and "maison" in desc.lower():
             item_loader.add_value("property_type", "house")
        elif desc and "studio" in desc.lower():
             item_loader.add_value("property_type", "studio")
        else:
            return

        item_loader.add_value("external_source", "Inli_PySpider_france")

        external_id = response.url.split('-')[-1].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)

        address = response.xpath("//li[contains(@class,'item__address')]/text()").get().upper()
        if address:
            item_loader.add_value("address", address.strip())
            city = "".join(response.xpath("substring-after(//h1/text(),', ')").extract())
            if city:
                city2 = city.upper()
                item_loader.add_value("city", city2) 
                zipcode = address.split(city2)[0].strip().split(" ")[-1]
                item_loader.add_value("zipcode", zipcode) 
            
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip().replace('\xa0', ''))

        description = " ".join(response.xpath("//p[@class='propos__description']//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

            if 'disponible à partir du' in description.lower():
                available_date = description.lower().split('disponible à partir du')[-1].split('(')[0].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"], languages=['fr'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        square_meters = response.xpath("//li[contains(text(),'m²')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", str(int(float(square_meters.split('m')[0].strip()))))

        room_count = response.xpath("substring-before(//ul[@class='propos__attributs__section__list']/li[contains(.,'chambre')],'chambre')").get()
        if room_count:
            room_count=room_count.strip()
            if int(room_count)>1:
               item_loader.add_value("room_count",room_count)
            else:
                room1=response.xpath("//div[contains(text(),'Pièce')]/following-sibling::div/text()").get()
                if room1:
                    item_loader.add_value("room_count", room1)

            
        
        rent = response.xpath("//span[contains(@class,'description--price')]/text()").get()
        if rent:
            item_loader.add_value("rent", str(int(float(rent.split('€')[0].strip().replace(' ', '').replace('\xa0', '')))))
            item_loader.add_value("currency", 'EUR')
        
        images = [x.strip('[').strip(']').strip('"').replace('\\', '') for x in response.xpath("//ui-galerie/@*[name()=':sources']").get().split(',')]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        latitude = response.xpath("//div[@id='mapFull']/ui-gmap/@*[name()=':center']").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('"lat":')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('"lng":')[1].split('}')[0].strip())
        
        floor = response.xpath("//div[contains(text(),'Étages')]/following-sibling::div/text()").get()
        if floor:
            if floor.strip().isnumeric():
                item_loader.add_value("floor", str(int(floor.strip())))
        
        parking = response.xpath("//div[contains(text(),'Parking')]/following-sibling::div/text()").get()
        if parking:
            if parking.strip().lower() == 'oui':
                item_loader.add_value("parking", True)
            elif parking.strip().lower() == 'non':
                item_loader.add_value("parking", False)

        elevator = response.xpath("//div[contains(text(),'Ascenseur')]/following-sibling::div/text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                item_loader.add_value("elevator", True)
            elif elevator.strip().lower() == 'non':
                item_loader.add_value("elevator", False)
        item_loader.add_value("landlord_phone", '01 40 89 77 77')
        item_loader.add_value("landlord_name", "in'li")

        yield item_loader.load_item()
