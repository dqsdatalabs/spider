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
    name = 'sicilproperty_it' 
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Sicilproperty_PySpider_italy"
    start_urls = ['http://www.sicilproperty.it/immobili/immobili-in-affitto/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//figure/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"http://www.sicilproperty.it/immobili/immobili-in-affitto/page/{page}/"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = "".join(response.xpath("//div[@class='property-page-type']//text()").getall())
        if get_p_type_string(property_type.strip()):
            item_loader.add_value("property_type", get_p_type_string(property_type.strip()))
        else:
            return
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//title/text()").get()
        item_loader.add_value("title", title)
        
        external_id = response.xpath("//div[@class='property-page-id' and contains(.,'Codice')]/span/text()").get()
        item_loader.add_value("external_id", external_id)
        
        address = response.xpath("//span[@class='map-address']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-1].strip())
        
        square_meters = response.xpath("//span[strong[contains(.,'Superficie')]]/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)
        
        room_count = response.xpath("//span[strong[contains(.,'Camere')]]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        rent = response.xpath("//span[@class='property-page-price']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[1].strip())
        item_loader.add_value("currency", "EUR")
        
        floor = response.xpath("//span[strong[contains(.,'Piano')]]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        furnished = response.xpath("//span[strong[contains(.,'Arredamento')]]/text()").get()
        if furnished and "si" in furnished.lower():
            item_loader.add_value("furnished", True)
        
        balcony = response.xpath("//span[strong[contains(.,'balcon') or contains(.,'Balcon')]]/text()[not(contains(.,'0'))]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        energy_label = response.xpath("//span[strong[contains(.,'energetica')]]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
            
        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat:')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('lng:')[1].split(',')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        furnished=response.xpath("//strong[.='Arredamento:']/following-sibling::text()").get()
        if furnished and "Sì" in furnished:
            item_loader.add_value("furnished",True)
        parking=response.xpath("//strong[.='Posti auto:']/following-sibling::text()").get()
        if parking:
            item_loader.add_value("parking",True)
        
        images = [x for x in response.xpath("//ul[@class='slides']//@src").getall()]
        item_loader.add_value("images", images)
        
        desc = "".join(response.xpath("//h4[contains(.,'Descrizione')]/..//p//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        utilities=" ".join(response.xpath("//p[contains(.,'condominiali')]/text()").getall())
        if utilities:
            utilities=utilities.split("condominiali")[-1].split("mensili")[0].split(",")[0].split(" ")[-1]
            if utilities.isdigit():
                item_loader.add_value("utilities",utilities)
        
        washing_machine = response.xpath("//li[contains(.,'Lavanderia')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        item_loader.add_value("landlord_name", "SicilProperty")
        item_loader.add_value("landlord_phone", "+39 095 0934130")
        item_loader.add_value("landlord_email", "info@sicilproperty.it")
        
        yield item_loader.load_item()
        
def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("trilocale" in p_type_string.lower() or "house" in p_type_string.lower() or "villetta" in p_type_string.lower() or "villino" in p_type_string.lower() or "villa" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    elif p_type_string and "monolocale" in p_type_string.lower():
        return "studio"
    else:
        return None