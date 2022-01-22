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
    name = '65croisette_fr'
    execution_type='testing'
    country='france' 
    locale='fr'
    external_source="65croisette_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.65croisette.fr/location+immobilier.html",
                ],
            },

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='clearfix annonces-container layout-view-mode-liste']//div[@class='lo-image-inner-cadre']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item,)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        if "parking" in title:
            return 
        rent=response.xpath("//div[@class='lo-box-header clearfix']/h1/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("-")[-1].split("€")[0].strip().replace("\xa0",""))
        item_loader.add_value("currency","EUR")
        adres=response.xpath("//div[@class='lo-box-header clearfix']/h1/span/text()").get()
        if adres:
            item_loader.add_value("address",adres.split("/")[0].strip())
        city=adres.split("-")[0].strip()
        if city:
            item_loader.add_value("city",city)
        zipcode=adres.split("-")[-1]
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        external_id=response.xpath("//div[@class='lo-box-header clearfix']/h1/span/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split("Réf. :")[-1].strip())
        description="".join(response.xpath("//div[@class='lo-box-content clearfix']/p/text()").getall())
        if description:
            item_loader.add_value("description",description.replace("\n\t\t\t\t\t\t\t","").replace("\n",""))
        property_type=response.xpath("//ul[@class='details-descriptif-liste']//li[1]/span[@class='details-descriptif-liste-span']/text()").get()
        if property_type:
            if property_type=="Appartement":
                item_loader.add_value("property_type","apartment")
        square_meters=response.xpath("//span[contains(.,'Surface')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("de")[-1].split(",")[0].split("m2")[0])
        room_count=response.xpath("//span[contains(.,'Pièce')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(" ")[0])
        deposit=response.xpath("//p[contains(.,'Dépôt de garantie :')]/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].split("€")[0].split(",")[0].replace(" ","").strip())
        utilities=response.xpath("//p[contains(.,'Honoraires charges locataire :')]/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].split("€")[0].split(",")[0].replace(" ","").strip())
        elevator=response.xpath("//span[.='Ascenseur']").get()
        if elevator:
            item_loader.add_value("elevator",True)
        floor=response.xpath("//span[contains(.,'Etage')]/text()").get()
        if floor:
            item_loader.add_value("floor",floor.split(":")[-1].strip())
        images=[x for x in response.xpath("//img[contains(@src,'files_nas')]/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        
        item_loader.add_value("landlord_name","65 Croisette")
        item_loader.add_value("landlord_phone","04.93.94.00.02")
        item_loader.add_value("landlord_email","contact@65croisette.fr")
        yield item_loader.load_item()