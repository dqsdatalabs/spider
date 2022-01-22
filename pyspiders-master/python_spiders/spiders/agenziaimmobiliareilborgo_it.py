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
    name = 'agenziaimmobiliareilborgo_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Agenziaimmobiliareilborgo_PySpider_italy"
    start_urls = ['http://agenziaimmobiliareilborgo.it/immobili.asp?go=1&ag=&r=&p=&v=&chm=&mq=&rif=&zn=&vt=&plan=&ft=&w=&pchiave=&pr=0&ct=0&c=0&t=0&l=0&pz=0&Ricerca=ricerca']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//table[contains(@id,'sw_annuncio')]"):
            follow_url = response.urljoin(item.xpath(".//a/@href[contains(.,'dettagli')]").get())
            prop_type = item.xpath(".//h2[contains(.,'Tipologia')]/following-sibling::h3[1]/text()").get()
            if get_p_type_string(prop_type):
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(prop_type)})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)

        rent=response.xpath("//h4[contains(.,'Notizie generali immobile')]//following-sibling::div[contains(.,'Prezzo:')]//following-sibling::div[1]//text()").get()
        if rent:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        address=response.xpath("//div[@class='sw_dati']//h2//strong//text()").get()
        if address:
            item_loader.add_value("address",address)

        city=response.xpath("//div[@class='sw_dati']//h2//strong//text()").get()
        if city:
            item_loader.add_value("city",city)

        desc=response.xpath("//div[contains(@class,'sw_dati')]//h2//text()").getall()
        if desc:
            item_loader.add_value("description",desc)

        room_count=response.xpath("//h4[contains(.,'Descrizione immobile')]//following-sibling::div[contains(.,'Numero locali:')]//following-sibling::div[1]//text()").get()
        if room_count:
            if not room_count.isalpha():
                item_loader.add_value("room_count",room_count.split("Locali"))
            else:
                room_count=response.xpath("//h4[contains(.,'Descrizione immobile')]//following-sibling::div[contains(.,'Numero camere:')]//following-sibling::div[1]//text()").get()
                if room_count:
                    item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//h4[contains(.,'Descrizione immobile')]//following-sibling::div[contains(.,'Numero bagni:')]//following-sibling::div[1]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        square_meters=response.xpath("//h4[contains(.,'Descrizione immobile')]//following-sibling::div[contains(.,'Mq:')]//following-sibling::div[1]//text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)

        energy_label=response.xpath("//h4[contains(.,'Classificazione Energetica')]//following-sibling::div[contains(.,'Classe Energetica')]//following-sibling::div[1]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split("(")[0])

        images = [response.urljoin(x) for x in response.xpath("//select[@name='picture']//option//@value").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "IMMOBILIARE IL BORGO")
        item_loader.add_value("landlord_phone", "0102463273")
        item_loader.add_value("landlord_email","info@agenziaimmobiliareilborgo.it")

        external_id = (response.url).split("id=")[-1]
        item_loader.add_value("external_id",external_id)

        utilities = response.xpath("//h4[contains(.,'Descrizione immobile')]//following-sibling::div[contains(.,'Spese condominiali')]//following-sibling::div[1]//text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities)

        furnished = response.xpath("//h4[contains(.,'Descrizione immobile')]//following-sibling::div[contains(.,'Arredato')]//following-sibling::div[1]//text()[.='Si']").get()
        if furnished:
            item_loader.add_value("furnished",True)            


        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "loft" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    else:
        return None