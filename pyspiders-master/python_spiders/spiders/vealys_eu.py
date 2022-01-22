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
    name = 'vealys_eu'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source='Vealys_PySpider_france'

    def start_requests(self):

        url = "https://www.vealys.eu/fr/liste.htm?ope=2#page=1&tri=PRIXD&ListeViewBienForm=text&ope=2"
        yield Request(url,callback=self.parse,)

    # 1. FOLLOWING
    def parse(self, response):
        border=response.xpath("//span[@class='nav-page-position']/text()").get()
        border=border.split("/")[-1].strip()
        page = response.meta.get("page", 2)
        seen = False
        for item in  response.xpath("//div[@class='liste-bien-photo']/div/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        if page == 2 or seen:
            if int(border)>=page:
                next_page = f"https://www.vealys.eu/fr/liste.htm?page={page}&tri=PRIXD&ListeViewBienForm=text&ope=2&lieu-alentour=0#tri=PRIXD&page={page}&ListeViewBienForm=text&ope=2"
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
        property_type=response.xpath("//h1[@class='detail-bien-type']/text()").get()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        adres=response.xpath("//h2[@class='detail-bien-ville']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        city=response.xpath("//h2[@class='detail-bien-ville']/text()").get()
        if city:
            item_loader.add_value("city",city.split("(")[0])
        zipcode=response.xpath("//h2[@class='detail-bien-ville']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split("(")[-1].split(")")[0])
        external_id=response.xpath("//span[.='Ref']/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        description="".join(response.xpath("//div[@class='detail-bien-desc-content clearfix']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        rent=response.xpath("//div[@class='detail-bien-prix']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip().replace(" ",""))
        item_loader.add_value("currency","GBP")
        room_count=response.xpath("//span[@class='ico-piece']/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0])
        images=[x for x in response.xpath("//img[@class='photo-slideshow photo-thumbs']//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        deposit=response.xpath("//div[@class='detail-bien-intro clearfix']//span[contains(.,'Dépôt de garantie')]/following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit)
        utilities=response.xpath("//div[@class='detail-bien-intro clearfix']//span[contains(.,'Honoraires charge locataire')]/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities)
        item_loader.add_value("landlord_name","Vealys Immobilier")
        
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