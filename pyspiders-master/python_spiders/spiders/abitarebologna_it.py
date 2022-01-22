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
    name = 'abitarebologna_it'
    external_source = "Abitarebologna_PySpider_italy"
    start_urls = ['https://www.abitarebologna.it/immobili-annunci.php']  # LEVEL 1

    formdata = {
        "inaffitto": "SI",
        "area": "",
        "mq": "",
        "vani": "",
        "giardino": "",
        "tipologia": "Abitativo",
        "prezzo1": "",
        "prezzo2": "",
        "codice": "",
    }
    
    def start_requests(self):
        
        yield FormRequest(
            url=self.start_urls[0],
            callback=self.parse,
            formdata=self.formdata
        )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(.,'Scheda immobile')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//strong[contains(.,'Tipologia')]/parent::div/following-sibling::text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            return
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title//text()").getall()
        if title:
            item_loader.add_value("title",title)
        desc=response.xpath("//strong[.='DESCRIZIONE']/parent::div/following-sibling::div/text()").get()
        if desc:
            item_loader.add_value("description",desc)
        rent=response.xpath("//strong[.='Prezzo:']/parent::div/following-sibling::text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("&nbsp;")[-1].split(",")[0].replace("€",""))
        item_loader.add_value("currency","EUR")
        
        images=response.xpath("//ul[@class='products']//li//a//img//@src").getall()
        if images:
            item_loader.add_value("images",images)
        room=response.xpath("//strong[.='Numero vani:']/parent::div/following-sibling::text()").get()
        if room:
            item_loader.add_value("room_count",room)
        floor=response.xpath("//strong[.='Piano:']/parent::div/following-sibling::text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        item_loader.add_value("landlord_name","Abitare Bologna")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("bilocale" in p_type_string.lower() or "casa" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house" 
    else:
        return None