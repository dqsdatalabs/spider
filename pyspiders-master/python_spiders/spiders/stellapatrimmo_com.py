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
    name = 'stellapatrimmo_com'
    execution_type='testing'
    country='france' 
    locale='fr'
    external_source="Stellapatrimmo_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.stellapatrimmo.com/catalog/advanced_search_result.php?action=update_search&search_id=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MAX=&C_33_MAX=&C_38_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&keywords=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.stellapatrimmo.com/catalog/advanced_search_result.php?action=update_search&search_id=1721185328348773&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=17&C_27_tmp=17&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_MAX=&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_34_MAX=&C_33_MAX=&C_38_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&keywords=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='link-product']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//h1[@class='product-title']/text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//h1[@class='product-title']/span/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        city=response.xpath("//h1[@class='product-title']/span/text()").get()
        if city:
            item_loader.add_value("city",city.strip().split(" ")[0])
        zipcode=response.xpath("//h1[@class='product-title']/span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip().split(" ")[-1])
        rent=response.xpath("//div[.='Loyer mensuel HC']/following-sibling::div/b/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("EUR")[0].strip())
        item_loader.add_value("currency","EUR")
        utilities=response.xpath("//div[.='Honoraires Locataire']/following-sibling::div/b/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("EUR")[0].strip().split(".")[0])
        deposit=response.xpath("//div[.='Dépôt de Garantie']/following-sibling::div/b/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("EUR")[0].strip().split(".")[0])
        square_meters=response.xpath("//div[.='Surface']/following-sibling::div/b/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m2")[0].split(".")[0])
        floor=response.xpath("//div[.='Etage']/following-sibling::div/b/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        room_count=response.xpath("//div[.='Nombre pièces']/following-sibling::div/b/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        elevator=response.xpath("//div[.='Ascenseur']/following-sibling::div/b/text()").get()
        if elevator and elevator=="Oui":
            item_loader.add_value("elevator",True)
        description="".join(response.xpath("//div[@class='products-description']/text()").getall())
        if description:
            item_loader.add_value("description",description)
        external_id=response.xpath("//div[@class='product-model']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.strip().split(" ")[-1])
        images=[x for x in response.xpath("//img[contains(@src,'office12')]/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","STELLA PATR'IMMO")
        item_loader.add_value("landlord_phone","04.95.21.08.19")



        yield item_loader.load_item()