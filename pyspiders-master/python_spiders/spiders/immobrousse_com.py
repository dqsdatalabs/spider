# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import datetime
from datetime import date
import dateparser

class MySpider(Spider):
    name = 'immobrousse_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    start_urls = ["https://www.immobrousse.com/achat-location-maison-appartement-brousse-immobilier-agence-immobiliere-brive-la-gaillarde?mode=get&achatlocation=location"]
    external_source='Immobrousse_PySpider_france'

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        
        for item in response.xpath("//div[@class='card-btn']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        if page == 2 or seen:
            next_page = f"https://www.immobrousse.com/achat-location-maison-appartement-brousse-immobilier-agence-immobiliere-brive-la-gaillarde?page={page}&form=true"
            if next_page:
                yield Request(
                next_page,
                callback=self.parse,
                meta={"page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        dontallow=response.url
        if "commercial" in dontallow or "bureaux" in dontallow:
            return
        property_type =response.url
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        
        rent=response.xpath("//h3[@class='mbr-section-title display-2']/text()").get()
        if rent:
            rent=rent.split("€")[0].split("-")[1].strip()
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","GBP")
        adres=response.xpath("//h3[@class='mbr-section-title display-2']/following-sibling::small/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        description=response.xpath("//div[@class='col-xs-12 col-md-4 lead']/p/text()").getall()
        if description:
            item_loader.add_value("description",description)
        external_id=response.xpath("//text()[contains(.,'Référence')]").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1])
        room_count=response.xpath("//text()[contains(.,'Nb. de pièces')]").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(":")[-1])
        square_meters=response.xpath("//text()[contains(.,'Surface habitable')]").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(":")[-1].split("m²")[0])
        deposit=response.xpath("//text()[contains(.,'Dépôt de Garanti')]").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].split("€")[0])
        utilities=response.xpath("//text()[contains(.,'Honoraires Charge Locataire')]").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].split("€")[0])
        images=[response.urljoin(x) for x in response.xpath("//div[@class='container-slide']//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","BROUSSE IMMOBILIER")
        item_loader.add_value("landlord_phone","05.55.17.73.80")
        item_loader.add_value("landlord_email","brousseimmobilier@orange.fr")        


        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "maison" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None