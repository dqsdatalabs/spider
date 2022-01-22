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
    name = 'immo-services-var_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="ImmoServicesVar_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.actimmoprovence.com/location/1",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        for item in  response.xpath("//li[@class='properties-list-v1__item']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//div[@class='title__text']//text()").get()
        if title:
            item_loader.add_value("title",title)
        external_id=response.xpath("//div[@class='properties-thumb-v1__reference']/span/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        property_type=response.xpath("//title//text()").get()
        if property_type and "Duplex" in property_type:
            item_loader.add_value("property_type","house")

        city=response.xpath("//div[.='Ville']/following-sibling::div/text()").get()
        if city:
            item_loader.add_value("city",city.split("(")[0].strip())
        zipcode=response.xpath("//div[.='Code postal']/following-sibling::div/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        adres=city+" "+zipcode
        if adres:
            item_loader.add_value("address",adres)
        images=[x for x in response.xpath("//picture/img/@data-src").getall()]
        if images:
            item_loader.add_value("images",images)
        description=response.xpath("//div[@class='properties-details-general-v3__description']//text()").getall()
        if description:
            item_loader.add_value("description",description)
        room_count=response.xpath("//div[.='Nombre de pièces']/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        square_meters=response.xpath("//div[.='Surface habitable (m²)']/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip())
        furnished=response.xpath("//div[.='Meublé']/following-sibling::div/text()").get()
        if furnished and "oui" in furnished.lower():
            item_loader.add_value("furnished",True)
        if furnished and "non" in furnished.lower():
            item_loader.add_value("furnished",False)

        elevator=response.xpath("//div[.='Ascenseur']/following-sibling::div/text()").get()
        if elevator and "oui" in elevator.lower():
            item_loader.add_value("furnished",True)
        if elevator and "non" in elevator.lower():
            item_loader.add_value("furnished",False)

        terrace=response.xpath("//div[.='Terrasse']/following-sibling::div/text()").get()
        if terrace and "oui" in terrace.lower():
            item_loader.add_value("terrace",True)
        if terrace and "non" in terrace.lower():
            item_loader.add_value("terrace",False)
        
        rent=response.xpath("//div[.='Loyer CC* / mois']/following-sibling::div/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0])
        item_loader.add_value("currency","EUR")
        utilities=response.xpath("//div[.='Honoraires TTC charge locataire']/following-sibling::div/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0])
        deposit=response.xpath("//p[contains(.,'Dépôt de garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("Dépôt de garantie")[-1].split("€")[0])
        item_loader.add_value("landlord_name","Act Immo Provence")
 


        yield item_loader.load_item()