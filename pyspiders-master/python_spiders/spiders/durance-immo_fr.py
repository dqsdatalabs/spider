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
    name = 'durance-immo_fr'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    start_urls = ["https://durance-immo.fr/locations"]
    external_source='DuranceImmo_PySpider_france'

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//figure[@class='mosaic-block']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//h3[@class='titling']/text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=response.xpath("//h3[@class='titling']/text()").get()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        images=[x for x in response.xpath("//ul[@class='elastislide-list']//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description="".join(response.xpath("//p[@class='padded intro']/text()").get())
        if description:
            item_loader.add_value("description",description)
        room_count=response.xpath("//th[.='Nb de chambres']/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        latitude=response.xpath("//script[contains(.,'lon')]/text()[1]").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("var lat")[-1].split(";")[0].replace("=","").strip())
        longitude=response.xpath("//script[contains(.,'lon')]/text()[1]").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("var lon")[-1].split(";")[0].replace("=","").strip())
        item_loader.add_value("landlord_name","Durance Immobilier")
        item_loader.add_value("landlord_phone","04 92 21 82 96")
        item_loader.add_value("landlord_email","duranceimmo@sfr.fr")
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