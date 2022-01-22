# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags 
from python_spiders.loaders import ListingLoader
import json
import re 

class MySpider(Spider): 
    name = 'saintpierre-immobilier_com' 
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="SaintpierreImmobilier_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.saintpierre-immobilier.com/location",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,callback=self.parse,)

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//section//figure//a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item) 
        
    
    
    # 2. SCRAPING level 2 
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/location/")[-1].split("-")[0])

        item_loader.add_xpath("title","//title//text()")
        f_text = response.url
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))

        rent=response.xpath("//section[contains(.,'mois')]/b/text()").get()
        if rent:
            rent=rent.split("€")[0].replace("\xa0","")
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        adres=response.xpath("//section[contains(.,'mois')]/h1/span[2]/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        city=response.xpath("//section[contains(.,'mois')]/h1/span[2]/text()").get()
        if city:
            item_loader.add_value("city",city.split("(")[0].strip())
        zipcode=response.xpath("//section[contains(.,'mois')]/h1/span[2]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split("(")[1].split(")")[0])
        images=[response.urljoin(x) for x in response.xpath("//picture//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description=response.xpath("//h3[.='Descriptif du bien']/following-sibling::p/text()").get()
        if description:
            item_loader.add_value("description",description)
        room_count=response.xpath("//li[contains(.,'pièce')]/text()").get()
        if room_count: 
            item_loader.add_value("room_count",room_count.split(" ")[0])
        bathroom_count=response.xpath("//li[contains(.,'salle d')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split(" ")[0])
        square_meters=response.xpath("//li[contains(.,'Surface')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(":")[1].split("m²")[0].strip())
        furnished=response.xpath("//li[contains(.,'Meublé')]/text()").get()
        if furnished:
            item_loader.add_value("furnished",True)
        parking=response.xpath("//li[contains(.,'parking')]/text()").get()
        if furnished:
            item_loader.add_value("parking",True)
        energy_label=response.xpath("//article//dl[@class='dpe']//dd[1]/b/@class").extract()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
        deposit=response.xpath("//li[contains(.,'Dépôt de garantie ')]/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].split("€")[0].replace("\xa0","").replace(" ",""))
        utilities=response.xpath("//li[contains(.,'Honoraires charge locataire')]/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].split("€")[0].replace("\xa0","").replace(" ",""))
        item_loader.add_value("landlord_name","Saint Pierre Immobilier")
        item_loader.add_value("landlord_phone","0 561 470 330")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower() or "maisonet" in p_type_string.lower() or "maison" in p_type_string.lower()):
        return "house"    
    else:
        return None