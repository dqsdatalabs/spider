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
    name = 'mjb-immobilier_com'
    execution_type='testing'
    country='france' 
    locale='fr'
    external_source="Mjbimmobilier_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.mjb-immobilier.com/annonces-immobilieres/tous-les-departements/tous-les-arrondissements/location/tous-types-de-biens/tous-les-prix/listing.html",
                ],

            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,callback=self.parse,)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='row-col listing-item-block-col listing-item-img text-left-d']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item,)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//div[contains(@class,'listing-item-category text-left text-uppercase')]/p/text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//h2[@class='pull-left lst-itm-dtl-header-txt3']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        city=adres.split("(")[0]
        if city:
            item_loader.add_value("city",city.strip())
        zipcode=adres.split("(")[-1].split(")")[0]
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        rent=response.xpath("//b[contains(.,'Prix')]/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split(":")[-1].strip().replace(" ",""))
        item_loader.add_value("currency","EUR")
        description=response.xpath("//p[@class='col-xs-12 pad-0']//text()").getall()
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//figure//a//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        property_type=title
        if property_type and "Appartement" in property_type:
            item_loader.add_value("property_type","apartment")
        if property_type and "Villa" in property_type:
            item_loader.add_value("property_type","house")
        square_meters=response.xpath("//li[contains(.,'Surf. hab.')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(":")[-1].split("m²")[0].strip())
        room_count=response.xpath("//li[contains(.,'Nombre de chambres')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(":")[-1].strip())
        utilities=response.xpath("//li[contains(.,'Honoraires TTC')]/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].split(".")[0])
        deposit=response.xpath("//li[contains(.,'Dépôt de garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].split(".")[0])
        furnished=response.xpath("//li[contains(.,'Meublé')]/text()").get()
        if furnished and "Oui" in furnished:
            item_loader.add_value("furnished",True)
        item_loader.add_value("landlord_name","MJB Immobilier")
        item_loader.add_value("landlord_phone","04.42.38.32.89")
        yield item_loader.load_item()