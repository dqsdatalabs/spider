# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'metroquadro_eu'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Metroquadro_PySpider_italy"
    start_urls = ['http://metroquadro.eu/elenco/in_Affitto/tutte_le_categorie/tutte_le_tipologie/tutti_i_comuni/?idbody=affitto']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@id='esito']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        prop_type = response.xpath("//li[contains(.,'Tipologia')]/strong/text()").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else:
            return
        item_loader.add_value("external_source", self.external_source)

        description = response.xpath("//div[@class='entry-dettaglio']/p/text()").get()
        if description:
            item_loader.add_value("description",description)

        square_meters = response.xpath("//li[contains(text(),'mq')]/strong/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)

        images = response.xpath("//img[@u='image']/@src").getall()
        if images:
            item_loader.add_value("external_images_count",len(images))
            item_loader.add_value("images",images)

        bathroom_count = response.xpath("//li[contains(text(),'Bagni')]/strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        room_count = response.xpath("//li[contains(text(),'Camere')]/strong/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        floor = response.xpath("//li[contains(text(),'Piano')]/strong/text()").get()
        if floor:
            item_loader.add_value("floor",floor[0])
        
        elevator = response.xpath("//li[@class='caratteristiche-li2']/strong/text()").get()
        if elevator:
            if "Ascensore" in elevator:
                item_loader.add_value("elevator",True)
            else:
                item_loader.add_value("elevator",False)

        
        energy_label = response.xpath("//li[contains(text(),'energetica')]/strong/text()").get()
        if energy_label:
            energy_label = energy_label.split()[1]
            item_loader.add_value("energy_label",energy_label)

        address = response.xpath("//h4[contains(text(),'-')]/text()").get()
        if address:
            address = address.split("-")[-1].strip()
            item_loader.add_value("address",address)
            item_loader.add_value("title",address)

        city = response.xpath("//li[contains(text(),'Comune')]/strong/text()").get()
        if city:
            item_loader.add_value("city",city)

        rent = response.xpath("//li[contains(text(),'Prezzo')]/strong/text()").get()
        if rent:
            if "riservata" in str(rent):
                return
            
            else:
                
                rent = rent.split()[0].split(",")[0]
                item_loader.add_value("rent",rent)       

        position = response.xpath("//script[contains(text(),'MappaParametri')]/text()").get()
        if position:
            lat, long =  position.split("&Longitudine=")  
            latitude = lat[-10:]         
            longitude = long[:10]
            item_loader.add_value("latitude",latitude)
            item_loader.add_value("longitude",longitude)

        utilities = response.xpath("//li[contains(text(),'annue')]/strong/text()").get()
        if utilities:
            utilities = utilities.split(",")[0]
            item_loader.add_value("utilities",utilities)

        external_id = response.xpath("//li[contains(text(),'Riferimento')]/strong/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)

        
        item_loader.add_value("currency","EUR")
        item_loader.add_value("landlord_name","Metroquadro Immobiliare s.r.l.")
        item_loader.add_value("landlord_email","info@metroquadro.eu")
        item_loader.add_value("landlord_phone","02207650371")

        

        yield item_loader.load_item()
    

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartament" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "casa" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    elif p_type_string and "en-suite" in p_type_string.lower():
        return "room"
    else:
        return None

