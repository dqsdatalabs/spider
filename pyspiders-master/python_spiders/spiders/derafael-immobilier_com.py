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
    name = 'derafael-immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="DerafaelImmobilier_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://derafael-immobilier.com/index.php?ctypmandatmeta=l&action=list&reference=&surface_habitable_min=&chambre_min=&prix_max=&orderby=bien.dcre+desc",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='img-wr']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=response.xpath("//h3[@class='headline']/text()").get()
        if property_type:
            if "Maison" in property_type:
                item_loader.add_value("property_type","house")
            if "Appartement" in property_type:
                item_loader.add_value("property_type","apartment")
        description="".join(response.xpath("//div[@id='desc']//p//text()").getall())
        if description:
            item_loader.add_value("description",description.replace("\t","").replace("\n","").replace("\r","").strip())
        rent=response.xpath("//p[contains(.,'Prix')]/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split(":")[-1].split("€")[0].replace("\xa0","").strip())
        item_loader.add_value("currency","EUR")
        adres=response.xpath("//p[contains(.,'Localité(s)')]/text()").get()
        if adres:
            item_loader.add_value("address",adres.split("Localité(s)")[-1].split(":")[-1])
        zipcode=response.xpath("//p[contains(.,'Localité(s)')]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split("-")[0].split(":")[1].strip())
        images=[x for x in response.xpath("//li/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        external_id=response.xpath("//p[contains(.,'Réf.')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1])
        room_count=response.xpath("//li[contains(.,'Pièce')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("Pièce")[0].strip())
        square_meters=response.xpath("//li[contains(.,'Surface habitable')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(":")[-1].split("m²")[0].replace("\xa0",""))
        deposit="".join(response.xpath("//div[@id='desc']//p//text()").getall())
        if deposit and "Dépôt de Gtie" in deposit:
            deposit=deposit.split("Dépôt de Gtie")[-1].split("€")[0].replace(":","").split(",")[0]
            item_loader.add_value("deposit",deposit)
        utilities="".join(response.xpath("//div[@id='desc']//p//text()").getall())
        if utilities and "Honoraires de location" in utilities:
            utilities=deposit.split("Honoraires de location")[-1].split("€")[0].replace(":","").split(",")[0]
            item_loader.add_value("utilities",utilities)
        item_loader.add_value("landlord_name","DE RAFAEL IMMOBILIER")
        item_loader.add_value("landlord_phone","06.60.88.46.23")

        yield item_loader.load_item()