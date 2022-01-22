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
    name = 'ladresse-le-havre_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    start_urls = ["https://www.ladresse-le-havre.com/catalog/result_carto.php?action=update_search&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&site-agence=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30_MIN=&30_MAX="]
    external_source='LadresseLeHavre_PySpider_france'

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='products-cell']/div/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)
        dontallow=response.xpath("//title/text()").get()
        if dontallow and "parking" in dontallow.lower():
            return 
        rent=response.xpath("//span[@class='alur_loyer_price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("Loyer")[1].split("€")[0].strip().replace("\xa0",""))
        item_loader.add_value("currency","GBP")
        property_type=response.xpath("//title/text()").get()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        room_count=response.xpath("//span[contains(.,'Pièces')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0])
        bathroom_count=response.xpath("//img[contains(@src,'picto-bain')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        square_meters=response.xpath("//span[contains(.,'m²')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].split(".")[0])
        energy_label=response.xpath("//div[@class='product-dpe']/div/img/@src").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split("dpe-")[-1].split(".")[0].upper())
        images=[response.urljoin(x) for x in response.xpath("//ul[@class='slides']//li//img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        desc="".join(response.xpath("//div[@class='content-desc']//text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        adres=response.xpath("//span[@class='alur_location_ville']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
            item_loader.add_value("zipcode",adres.split(" ")[0])
            item_loader.add_value("city",adres.split(" ")[1:])
        deposit=response.xpath("//span[@class='alur_location_depot']/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].split("€")[0].strip().replace("\xa0",""))
        utilities=response.xpath("//span[@class='alur_location_honos']/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].split("€")[0].strip().replace("\xa0",""))
        item_loader.add_value("landlord_name","L'Adresse Immobilier")
        item_loader.add_value("landlord_phone"," 02 35 42 48 87")
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