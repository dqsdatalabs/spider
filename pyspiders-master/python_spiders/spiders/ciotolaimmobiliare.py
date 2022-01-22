# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'ciotolaimmobiliare'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Ciotolaimmobiliare_PySpider_italy"
    start_urls = ['http://ciotolaimmobiliare.com/elenco.aspx?tipoOfferta=33&comune=63049&prezzo=0&ric_libera=&zona=0&contratto=2']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'holder')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"http://ciotolaimmobiliare.com/elenco.aspx?tipoOfferta=33&prezzo=0&ric_libera=&zona=0&n=5&p={page}&ord=0&contratto=2&comune=63049"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("-")[-1].split(".")[0])
        f_text = response.xpath("//h1/text()").get()
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title)
            address = title.split("|")[-1].strip()
            item_loader.add_value("address", address)
        
        rent = response.xpath("//li[span[contains(.,'Prezzo')]]/span[2]/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[1].strip())
        item_loader.add_value("currency", "EUR")
        city=response.xpath("//h1[@class='prop-title pull-center margin0']/text()").get()
        if city:
            item_loader.add_value("city",city.split("|")[-1].strip().split(" ")[0].capitalize())
        
        square_meters = response.xpath("//li[span[contains(.,'Metratura')]]/span[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        room_count = response.xpath("//li[span[contains(.,'Camere')]]/span[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//li[span[contains(.,'Bagni')]]/span[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        elevator = response.xpath("//li[span]/span[2]/text()[contains(.,'ascensore')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//li[span[contains(.,'Balcon')]]/span[2]/text()[.!='0']").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        energy_label = response.xpath("//li[span[contains(.,'energetica')]]/span[2]/text()[.!='0']").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        description = "".join(response.xpath("//h3[contains(.,'Descrizione ')]/following-sibling::text()").getall())
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()  ))
        
        images = [x for x in response.xpath("//div[@class='fotorama']//@src").getall()]
        item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_value("landlord_name", "CIOTOLA IMMOBILIARE")
        
        landlord_phone = "".join(response.xpath("//div[@class='riga_contatto']/text()").getall())
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        else:
            item_loader.add_value("landlord_phone", "081.7280101")
        item_loader.add_value("landlord_email", "info@ciotolaimmobiliare.com")
        
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