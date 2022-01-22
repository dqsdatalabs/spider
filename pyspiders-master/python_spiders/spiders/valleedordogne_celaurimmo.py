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
    name = 'valleedordogne_celaurimmo'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = "Valleedordogne_Celaurimmo_PySpider_france"
    start_urls = ['https://valleedordogne.celaurimmo.com/catalogue/locations']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='produit-mask-lien']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            prop_type = item.xpath("./@title").get()
            if get_p_type_string(prop_type):
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(prop_type)})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        external_id=response.xpath("//div[contains(.,'Ref.')]/span/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        title=response.xpath("//meta[@name='DC.title']/@content").get()
        if title:
            item_loader.add_value("title",title)

        rent=response.xpath("//div[@class='prix_lib']/div/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0])
        desc=" ".join(response.xpath("//div[@class='desc texte_editable hyphenate']/text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        images=[x for x in response.xpath("//a[@class='petites']/@data-href").getall()]
        if images:
            item_loader.add_value("images",images)
        room_count=response.xpath("//div[.='Nombre de pièces  :']/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        square_meters=response.xpath("//div[.='Surface habitable :']/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0])
        deposit=response.xpath("//div[.='Dépôt de garantie :']/following-sibling::div/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0])
        utilities=response.xpath("//div[.='Charges :']/following-sibling::div/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0])
        furnished=response.xpath("//div[.='Meublé :']/following-sibling::div/text()").get()
        if furnished and "oui" in furnished.lower():
            item_loader.add_value("furnished",True)
        energy_label=response.xpath("//div[.='Consommation énergétique']/following-sibling::div/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
        
        item_loader.add_value("landlord_name","CELAUR IMMOBILIER")

        yield item_loader.load_item() 

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "duplex" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None