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
    name = 'primum-immobilier_fr'
    execution_type='testing'
    country='france' 
    locale='fr'
    external_source="Primumimmobilier_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.primum-immobilier.fr/catalog/advanced_search_result.php?action=update_search&search_id=1721118429132776&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_33_MAX=&C_30_MIN=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='btn-selection']/following-sibling::a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        city=response.xpath("//div[.='Ville']/following-sibling::div/b/text()").get()
        if city:
            item_loader.add_value("city",city)
        zipcode=response.xpath("//div[.='Code postal']/following-sibling::div/b/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        adres=city+" "+zipcode
        if adres:
            item_loader.add_value("address",adres)
        floor=response.xpath("//div[.='Etage']/following-sibling::div/b/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        rent=response.xpath("//div[.='Loyer mensuel HC']/following-sibling::div/b/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("EUR")[0])
        item_loader.add_value("currency","EUR")
        utilities=response.xpath("//div[.='Honoraires Locataire']/following-sibling::div/b/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("EUR")[0].strip())
        deposit=response.xpath("//div[.='Dépôt de Garantie']/following-sibling::div/b/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("EUR")[0].strip())
        room_count=response.xpath("//div[.='Nombre pièces']/following-sibling::div/b/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        furnished=response.xpath("//div[.='Meublé']/following-sibling::div/b/text()").get()
        if furnished and furnished=="Non":
            item_loader.add_value("furnished",False)
        square_meters=response.xpath("//div[.='Surface']/following-sibling::div/b/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m2")[0].strip().split(".")[0])
        description=response.xpath("//div[@class='product-description']/text()").getall()
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//div//img[contains(@src,'photos')]/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        available_date=response.xpath("//div[.='Disponibilité']/following-sibling::div/b/text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date)
        external_id=response.xpath("//span[contains(.,'Ref.')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1])
        energy_label=response.xpath("//div[.='Consommation énergie primaire']/following-sibling::div/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
        elevator=response.xpath("//div[.='Ascenseur']/following-sibling::div/b/text()").get()
        if elevator=="Non":
            item_loader.add_value("elevator",False)
        item_loader.add_value("landlord_name","PRIMUM IMMOBILIER")
        yield item_loader.load_item()