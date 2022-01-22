# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from itemadapter.utils import is_item
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
import re

class MySpider(Spider):
    name = 'lagence-esprit-immo_fr'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source='LagenceEspritImmo_PySpider_france'

    def start_requests(self):

        url = "http://www.lagence-esprit-immo.fr/catalog/advanced_search_result_carto.php?action=update_search&search_id=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=UNIQUE&C_27=&C_34_search=SUPERIEUR&C_34_type=NUMBER&C_34_MIN=&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&keywords=&map_polygone=&C_65_REPLACE=&C_65_search=CONTIENT&C_65_type=TEXT&C_65="
        yield Request(
            url,
            callback=self.parse,
         
        )

    # 1. FOLLOWING
    def parse(self, response):
        for item in  response.xpath("//div[@class='display-cell w100 verticalTop']//a//@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

        nextpage=response.xpath("//a[@class='page_suivante']/@href").get()
        if nextpage:
            yield Request(
            response.urljoin(nextpage),
            callback=self.parse,
        )


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        dontallow=response.url
        if dontallow and "facebook" in dontallow:
            return 
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=response.xpath("//h1[@class='text-uppercase']/text()").get()
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        adres=response.xpath("//h1[@class='text-uppercase']/text()").get()
        if adres:
            item_loader.add_value("address",adres.strip().split(" ")[-1])
        desc="".join(response.xpath("//div[@class='col-xs-12 col-sm-4']/text() | //div[@class='description text-justify']/h3/text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        external_id=response.xpath("//span[contains(.,'Ref')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())
        images=[x.split("url('..")[1].split(")")[0].replace("'","") for x in response.xpath("//div[@class='img']/@style").getall()]
        if images:
            item_loader.add_value("images",images)
        rent=response.xpath("//span[@class='alur_loyer_price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].split("Loyer")[1])
        item_loader.add_value("currency","GBP")
        room_count=response.xpath("//div[@class='display-table w100 carac']//div[contains(.,'Pièce')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0])
        square_meters=response.xpath("//div[@class='display-table w100 carac']//div[contains(.,'m²')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0])
        deposit=response.xpath("//span[contains(.,'Dépôt de garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].split("€")[0])
        utilities=response.xpath("//span[contains(.,'Honoraires charge locataire')]/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].split("€")[0])
        latitude=response.xpath("//script[contains(.,'google.maps.LatLng(')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("google.maps.LatLng(")[1].split(")")[0].split(",")[0])
        longitude=response.xpath("//script[contains(.,'google.maps.LatLng(')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("google.maps.LatLng(")[1].split(")")[0].split(",")[1])

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