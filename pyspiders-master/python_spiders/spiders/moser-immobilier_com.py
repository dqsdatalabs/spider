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
    name = 'moser-immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="MoserImmobilier_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.moser-immobilier.com/recherche-immobilier/location.html",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='row-btn-bloc-resultat']/following-sibling::a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        adres=response.xpath("//div[@class='col-xs-12 col-sm-12 col-md-12 col-lg-12']/h1/strong/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=response.xpath("//div[@class='col-xs-12 col-sm-12 col-md-12 col-lg-12']/h1/strong/following-sibling::text()").get()
        if property_type:
            if "Villa"==property_type:
                item_loader.add_value("property_type","house")
            if "Appartement" in property_type:
                item_loader.add_value("property_type","apartment")
        dontallow=response.url
        if dontallow and "commercial" in dontallow:
            return 
        external_id=response.xpath("//div[@class='col-xs-12 col-sm-12 col-md-12 col-lg-12']//text()[contains(.,'Réf')]").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())
        rent=response.xpath("//strong[contains(.,'Loyer HC')]/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split(":")[-1].split("€")[0].strip().replace(" ",""))
        item_loader.add_value("currency","EUR")
        utilities=response.xpath("//strong[contains(.,'Charges')]/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].split("€")[0].strip())
        deposit=response.xpath("//strong[contains(.,'Dépôt de garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].split("€")[0].strip().replace(" ",""))
        furnished= response.xpath("//text()[contains(.,'Meublé')]/following-sibling::strong/text()").get()
        if furnished and "non"==furnished.lower():
            item_loader.add_value("furnished",False)
        if furnished and "oui"==furnished.lower(): 
            item_loader.add_value("furnished",True)
        floor=response.xpath("//text()[contains(.,'Etage :')]/following-sibling::strong/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        room_count=response.xpath("//text()[contains(.,'Nbre de pièce(s)')]/following-sibling::strong/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        square_meters=response.xpath("//text()[contains(.,'Surface habitable')]/following-sibling::strong/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip())
        terrace=response.xpath("//text()[contains(.,'Terrasse')]/following-sibling::strong/text()").get()
        if terrace and "non"==terrace.lower():
            item_loader.add_value("terrace",False)
        if terrace and "oui"==terrace.lower():
            item_loader.add_value("terrace",True)
        parking=response.xpath("//text()[contains(.,'Nbre de parking')]/following-sibling::strong/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        elevator=response.xpath("//text()[contains(.,'Ascenseur')]/following-sibling::strong/text()").get()
        if elevator and "non"==elevator.lower():
            item_loader.add_value("elevator",False)
        if elevator and "oui"==elevator.lower():
            item_loader.add_value("elevator",True)
        images=[response.urljoin(x) for x in response.xpath("//div[@class='visible-print']//div[@class='row']//div//img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description="".join(response.xpath("//div[@itemprop='description']/text()").getall())
        if description:
            item_loader.add_value("description",description)
        energy_label=response.xpath("//u[.='Performance énergétique']/following-sibling::strong/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
            

        yield item_loader.load_item()