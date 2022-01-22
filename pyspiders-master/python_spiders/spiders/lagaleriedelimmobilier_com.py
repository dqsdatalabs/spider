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
    name = 'lagaleriedelimmobilier_com'
    execution_type='testing'
    country='france' 
    locale='fr'
    external_source="Lagaleriedelimmobilier_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.lagaleriedelimmobilier.com/fr/annonces/louer-p-r70-2-1.html#menuSave=2&page=1&TypeModeListeForm=text&ope=2&filtre=2",
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

        for item in response.xpath("//div[@class='liste-bien-photo']/div/a/@href").getall():
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
        rent=response.xpath("//div[@class='detail-bien-prix']/span/following-sibling::text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip().replace(" ","").split(".")[0])
        item_loader.add_value("currency","EUR")
        deposit="".join(response.xpath("//div[@class='detail-bien-desc-content clearfix']//p//text()").getall())
        if deposit:
            item_loader.add_value("deposit",deposit.split("Dépôt de garantie:")[-1].split("€")[0].strip())
        utilities="".join(response.xpath("//div[@class='detail-bien-desc-content clearfix']//p//text()").getall())
        if utilities:
            item_loader.add_value("utilities",utilities.split("Honoraires agence:")[-1].split("€")[0].strip())
        adres=response.xpath("//h2[@class='detail-bien-ville']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        city=adres.strip().split(" ")[0]
        if city:
            item_loader.add_value("city",city)
        zipcode=adres.strip().split(" ")[-1]
        if zipcode:
            item_loader.add_value("zipcode",zipcode.replace("(","").replace(")",""))
        square_meters=response.xpath("//svg[@class='icon icon-surface']/parent::span/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip())
        room_count=response.xpath("//svg[@class='icon icon-room']/parent::span/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0])
        description="".join(response.xpath("//div[@class='detail-bien-desc-content clearfix']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        external_id=response.xpath("//span[@itemprop='productID']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        energy_label=response.xpath("//img[@class='img-nrj img-dpe']/@src").get()
        if energy_label:
            energy_label=energy_label.split("=")[-1]
            if not energy_label.isalpha():
                e_label=energy_label_calculate(energy_label)
                item_loader.add_value("energy_label",e_label)
        images=[x for x in response.xpath("//img[@class='photo-slideshow photo-thumbs']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","La Galerie De L'immobilier")
        yield item_loader.load_item()


def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label