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
import re

class MySpider(Spider):
    name = 'poitou-immobilier_fr'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source='PoitouImmobilier_PySpider_france'
    custom_settings = {"HTTPCACHE_ENABLED":False}
    def start_requests(self):

        url = "http://www.poitou-immobilier.fr/resultats-bien-location.php"
        yield FormRequest(
            url,
            callback=self.parse,
         
        )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in  response.xpath("//div[@class='col2']//a/@href").getall():
            follow_url = response.urljoin(item)
            yield FormRequest(follow_url, callback=self.populate_item)


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        property_type =response.xpath("//h1//text()").get()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        title=response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title",title)
        square_meters=response.xpath("//h1//text()").get()
        if square_meters:
            square_meters=square_meters.split("-")
            for i in square_meters:
                if "m²" in i:
                    squ=re.findall("\d+",i)
                    if squ:
                        item_loader.add_value("square_meters",squ)

        
        description=response.xpath("//h1/following-sibling::p/text()").get()
        if description:
            item_loader.add_value("description",description)
        rent=response.xpath("//h1//text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("•")[-1].split("€")[0])
        item_loader.add_value("currency","GBP")
        deposit=response.xpath("//span[contains(.,'Dépot de garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].split("€")[0])
        utilities=response.xpath("//span[contains(.,'Honoraires')]/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].split("€")[0])
        external_id=response.xpath("//span[contains(.,'Réf.')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(".")[-1].strip())
        images=[x for x in response.xpath("//div[@class='ligne-vignettes']//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
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