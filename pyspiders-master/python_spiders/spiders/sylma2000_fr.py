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
import dateparser
class MySpider(Spider):
    name = 'sylma2000_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Sylma2000_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.sylma2000.fr/a-louer/1",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//article[@class='row panelBien']/@onclick").getall():
            item=item.split("href='")[-1].split("'")[0]
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//h2[@class='col-md-9 col-sm-12']/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ',title.strip()))

        property_type=response.xpath("//h2[@class='col-md-9 col-sm-12']/text()").get()
        if property_type:
            if "Villa" in property_type:
                item_loader.add_value("property_type","house")
            if "Appartement" in property_type:
                item_loader.add_value("property_type","apartment")
        
        adres=response.xpath("//p[contains(.,'Ville')]/span/text()").get()
        if adres:
            item_loader.add_value("address",adres.replace("\n","").strip())
        zipcode=response.xpath("//p[contains(.,'Code postal ')]/span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip())
        city=response.xpath("//p[contains(.,'Ville')]/span/text()").get()
        if city:
            item_loader.add_value("city",city.replace("\n","").strip())
        square_meters=response.xpath("//p[contains(.,'Surface habitable')]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].split(",")[0].strip())
        room_count=response.xpath("//p[contains(.,'Nombre de pièces')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        bathroom_count=response.xpath("//p[contains(.,'Nb de salle de bains')]/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        furnished=response.xpath("//p[contains(.,'Meublé')]/span/text()").get()
        if furnished and "non" in furnished.lower():
            item_loader.add_value("furnished",False)
        if furnished and "oui" in furnished.lower():
            item_loader.add_value("furnished",True)
        elevator=response.xpath("//p[contains(.,'Ascenseur ')]/span/text()").get()
        if elevator and "non" in elevator.lower():
            item_loader.add_value("elevator",False)
        if elevator and "oui" in elevator.lower():
            item_loader.add_value("elevator",True)
        floor=response.xpath("//p[contains(.,'Etage ')]/span/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        description=response.xpath("//p[@itemprop='description']/text()").get()
        if description:
            item_loader.add_value("description",description)
        rent=response.xpath("//span[@class='col-md-3 col-sm-12 prixdt4']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip().replace(" ",""))
        item_loader.add_value("currency","EUR")
        utilities=response.xpath("//p[contains(.,'Honoraires TTC charge locataire')]/span/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].strip())
        deposit=response.xpath("//p[contains(.,'Dépôt de garantie TTC')]/span/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0].strip())
        parking=response.xpath("//p[contains(.,'Nombre de garage')]/span/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        images=[x for x in response.xpath("//ul//li//img//@src").getall()] 
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","Agence Sylma 2000 - Le Raincy")
        item_loader.add_value("landlord_phone","01 43 09 09 09")
        item_loader.add_value("landlord_email","leraincy@sylma2000.fr")
        yield item_loader.load_item()