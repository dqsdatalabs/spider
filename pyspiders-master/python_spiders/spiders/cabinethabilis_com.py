# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import re
import math
from datetime import datetime

class MySpider(Spider):
    name = 'cabinethabilis_com'
    start_urls = ["https://www.cabinet-habilis.com/a-louer/1"]
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = 'Cabinethabilis_PySpider_france_fr'
    custom_settings = { 
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//li[contains(@class,'panelBien')]"):
            follow_url = response.urljoin(item.xpath(".//a[@class='btn btn-listing']/@href").get())
            prop_type = item.xpath(".//div[@class='bienTitle']/h2/text()").get()
            if "appartement" in prop_type.lower() or "studio" in prop_type.lower():
                prop_type = "apartment"
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': prop_type})
            elif "maison" in prop_type.lower() or "duplex" in prop_type.lower() or "villa" in prop_type.lower():
                prop_type = "house"
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': prop_type})
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.cabinet-habilis.com/a-louer/{page}"
            yield Request(url=url,
                                callback=self.parse,
                                meta={"page": page+1})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_xpath("title", "//div[@class='themTitle']/h1/text()")
        item_loader.add_value("property_type", response.meta.get("property_type"))

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)

        latitude_longitude = response.xpath("//script[contains(.,'geocoder')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat : ')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('lng:  ')[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        address = response.xpath("//p/span[contains(.,'Ville')]/following-sibling::span/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())
        
        zipcode = response.xpath("//p/span[contains(.,'Code')]/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        bathroom_count = response.xpath("//p/span[contains(.,'salle')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        square_meters = response.xpath("//span[contains(.,'Surface habitable')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('m')[0].strip().replace(',','.'))))
            item_loader.add_value("square_meters", square_meters)
       

        room_count = response.xpath("//span[contains(.,'Nombre de chambre')]/following-sibling::span/text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//span[contains(.,'Nombre de pièces') and @class='termInfos']/following-sibling::span/text()").get()
            if room_count:
                room_count = room_count.strip()
                item_loader.add_value("room_count", room_count)          

        rent = response.xpath("//span[contains(.,'Loyer')]/following-sibling::span/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[0].replace(" ",""))
        item_loader.add_value("currency","EUR")

        external_id = response.xpath("//li[@class='ref']/text()").get()
        if external_id:
            external_id = external_id.strip().strip('Ref').strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//p[@itemprop='description']/following-sibling::p/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d + ' '
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        images = [x for x in response.xpath("//ul[@class='imageGallery  loading']/li/@data-thumb").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//span[contains(.,'Dépôt')]/following-sibling::span/text()").get()
        if deposit:
            deposit = deposit.split('€')[0].strip().replace(' ', '')
            if deposit != '0':
                item_loader.add_value("deposit", deposit)

        furnished = response.xpath("//span[contains(.,'Meubl')]/following-sibling::span/text()").get()
        if furnished:
            if furnished.strip().lower() == 'non':
                furnished = False
            elif furnished.strip().lower() == 'oui':
                furnished = True
            if type(furnished) == bool:
                item_loader.add_value("furnished", furnished)

        floor = response.xpath("//span[contains(.,'Etage')]/following-sibling::span/text()").get()
        if floor:
            floor = floor.strip()
            item_loader.add_value("floor", floor)

        parking = response.xpath("//span[contains(.,'Nombre de parking')]/following-sibling::span/text()").get()
        if parking:
            if int(parking.strip()) > 0:
                parking = True
                item_loader.add_value("parking", parking)

        elevator = response.xpath("//span[contains(.,'Ascenseur')]/following-sibling::span/text()").get()
        if elevator:
            if elevator.strip().lower() == 'non':
                elevator = False
            elif elevator.strip().lower() == 'oui':
                elevator = True
            if type(elevator) == bool:
                item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//span[contains(.,'Balcon')]/following-sibling::span/text()").get()
        if balcony:
            if balcony.strip().lower() == 'non':
                balcony = False
            elif balcony.strip().lower() == 'oui':
                balcony = True
            if type(balcony) == bool:
                item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//span[contains(.,'Terrasse')]/following-sibling::span/text()").get()
        if terrace:
            if terrace.strip().lower() == 'non':
                terrace = False
            elif terrace.strip().lower() == 'oui':
                terrace = True
            if type(terrace) == bool:
                item_loader.add_value("terrace", terrace)
        
        utilities=response.xpath("//span[contains(.,'Honora')]/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].strip())
        
        item_loader.add_value("landlord_name", "CABINET HABILIS")
        
        landlord_phone = response.xpath("//span[@class='icon-telephone icons fs-35']/span/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        email=response.xpath("//p/a/@href[contains(.,'mail')]").get()
        if email:
            item_loader.add_value("landlord_email", email.split(":")[1])
        
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data