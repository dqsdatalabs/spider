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
    name = 'ekidom_fr'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source='Ekidom_PySpider_france'

    def start_requests(self):

        url = "https://www.ekidom.fr/trouvez-un-bien-a-louer/logement/?emplacement=&loyer_maximum=&nb_pieces="
        yield Request(url,callback=self.parse,)

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        
        
        for item in  response.xpath("//div[@class='bien']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        if page == 2 or seen:
            next_page = f"https://www.ekidom.fr/trouvez-un-bien-a-louer/logement/?page_num={page}&emplacement&loyer_maximum&nb_pieces"
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
        if dontallow and ("garage" in dontallow or "parking" in dontallow):
            return 
        title=response.xpath("//h1/a/text()").get()
        if title:
            item_loader.add_value("title",title)

        rent=response.xpath("//p[@class='prix']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].split(",")[0].strip())
        item_loader.add_value("currency","GBP")
        
        external_id=response.xpath("//p[@class='reference']/text()").get()
        if rent:
            item_loader.add_value("external_id",external_id.split(":")[-1]. strip())
        square_meters=response.xpath("//p[contains(.,'Surface')]/a/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0])
        room_count=response.xpath("//p[contains(.,'Nb. de pièce')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(":")[-1])
        property_type=response.xpath("//p[contains(.,'Type')]/a/text()").get()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        features=response.xpath("//p[.='Equipement(s) annexe(s) : ']/following-sibling::div//p//text()").getall()
        if features:
            for i in features:
                if "balcon" in i.lower():
                    item_loader.add_value("balcony",True)
                if "ascenseur" in i.lower():
                    item_loader.add_value("elevator",True)
        adres=response.xpath("//p[.='Situation géographique : ']/following-sibling::p/a/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        description="".join(response.xpath("//div[@class='texte_commercial']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        utilities=response.xpath("//p[@class='charges']/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].split("€")[0].split(",")[0])
        energy_label=response.xpath("//img[contains(@src,'gabarit')]/@src").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split("CE-")[-1].split(".")[0])

                
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