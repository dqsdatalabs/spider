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
    name = 'putignano_berardiimmobiliare_it'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = "PutignanoBerardiimmobiliare_PySpider_italy"
    start_urls = ['http://www.putignano-berardiimmobiliare.it/affitti/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//strong/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            prop_type = item.xpath("./text()").get()
            if get_p_type_string(prop_type):
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(prop_type)})
            # else:
            #     print(prop_type)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        
        external_id=response.xpath("//span[.='Codice']/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        address=response.xpath("//div[@class='sintesiAnnuncio']/text()").get()
        if address and "Indirizzo" in address:
            item_loader.add_value("address",address.split("Indirizzo")[-1])
        city=response.xpath("//div[@class='titleAnnuncio']/span[@itemprop='name']/text()").get()
        if city:
            item_loader.add_value("city",city.split(" ")[-1])
        title=response.xpath("//div[@class='titleAnnuncio']/span[@itemprop='name']/text()").get()
        if title:
            item_loader.add_value("title",title)

        rent=response.xpath("//span[@itemprop='price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[-1].strip())
        item_loader.add_value("currency","EUR")
        terrace=response.xpath("//span[.='Terrazzo']/following-sibling::text()").get()
        if terrace and "si" in terrace:
            item_loader.add_value("terrace",True)
        floor=response.xpath("//span[.='Piano']/following-sibling::text()").get()
        if floor and floor.isdigit():
            item_loader.add_value("floor",floor)


        square_meters=response.xpath("//span[.='Metri Quadri']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)
        bathroom_count=response.xpath("//span[.='Bagni']/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        room_count=response.xpath("//span[.='Locali']/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        utilities=response.xpath("//span[.='Spese cond. mese']/following-sibling::text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[-1].split(",")[0].strip())
        balcony=response.xpath("//span[.='Balcone']/following-sibling::text()").get()
        if balcony and "si"==balcony:
            item_loader.add_value("balcony",True)
        desc=response.xpath("//div[@id='descit']//text()").get()
        if desc:
            item_loader.add_value("description",desc)
        images=[x for x in response.xpath("//img[@itemprop='image']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","PUTIGNANO&BERARDI IMMOBILIARE")
        item_loader.add_value("landlord_phone","080637579")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartamento" in p_type_string.lower() or "bilocale" in p_type_string.lower() or "trilocale" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "attico" in p_type_string.lower():
        return "apartment"
    elif p_type_string and ("casa" in p_type_string.lower() or "loft" in p_type_string.lower() or "quadrilocale" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "monolocale" in p_type_string.lower():
        return "studio"
    else:
        return None