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
    name = 'algtimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Algtimmo_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.algtimmo.fr/location/1",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//button[contains(.,'Détails')]/@data-url").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        external_id=response.xpath("//p[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())
        rent=response.xpath("//p[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].split(",")[0].strip())
        item_loader.add_value("currency","EUR")
        adres=response.xpath("//li[contains(.,'Ville')]/text()").get()
        if adres:
            item_loader.add_value("address",adres.split(":")[-1].strip())
        city=response.xpath("//li[contains(.,'Ville')]/text()").get()
        if city:
            item_loader.add_value("city",city.split(":")[-1].strip())
        square_meters=response.xpath("//li[contains(.,'Surface habitable')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(":")[-1].split("m²")[0].split(",")[0].strip())
        room_count=response.xpath("//li[contains(.,'Nombre de pièces')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(":")[-1].strip())
        bathroom_count=response.xpath("//li[contains(.,'Nb de salle de bains')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split(":")[-1].strip())
        furnished=response.xpath("//li[contains(.,'Meublé')]/text()").get()
        if furnished and "NON" in furnished.split(":")[-1]:
            item_loader.add_value("furnished",False)
        if furnished and "OUI" in furnished.split(":")[-1]:
            item_loader.add_value("furnished",True)
        terrace=response.xpath("//li[contains(.,'Terrasse ')]/text()").get()
        if terrace and "NON" in terrace.split(":")[-1]:
            item_loader.add_value("terrace",False)
        if terrace and "OUI" in terrace.split(":")[-1]:
            item_loader.add_value("terrace",True)
        balcony=response.xpath("//li[contains(.,'Balcon')]/text()").get()
        if balcony and "NON" in balcony.split(":")[-1]:
            item_loader.add_value("balcony",False)
        if balcony and "OUI" in balcony.split(":")[-1]:
            item_loader.add_value("balcony",True)
        parking=response.xpath("//li[contains(.,'Nombre de parking')]/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        utilities=response.xpath("//li[contains(.,'Honoraires TTC charge locataire')]/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].split("€")[0].split(",")[0].strip())
        deposit=response.xpath("//li[contains(.,'Dépôt de garantie TTC')]/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].split("€")[0].split(",")[0].strip())
        description=response.xpath("//h2[contains(.,'Description de l')]/following-sibling::p/text()").get()
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//picture//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name"," ALGT IMMO")
        
        yield item_loader.load_item()