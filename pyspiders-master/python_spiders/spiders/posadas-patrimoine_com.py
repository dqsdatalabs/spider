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
    name = 'posadas-patrimoine_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="PosadasPatrimoine_PySpider_france"
    custom_settings = { 
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }
    formdata={
        "data[Search][offredem]": "2",
        "data[Search][piecesmin]": "",
        "data[Search][prixmin]": "",
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.posadas-patrimoine.com/recherche/",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield FormRequest(item,formdata=self.formdata,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):

        for item in  response.xpath("//div[@class='btn-primary btn-annonce']//a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)

        title=response.xpath("//h1[@class='titlepage']/span/text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=response.xpath("//h1[@class='titlepage']/span/text()").get()
        if property_type and "appartement" in property_type.lower():
            item_loader.add_value("property_type","apartment")
        if property_type and "maison" in property_type.lower():
            item_loader.add_value("property_type","house")
        external_id=response.xpath("//div[@class='ref-annonces']/b/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        address=response.xpath("//div[@class='ref-annonces']/span/text()").get()
        if address:
            item_loader.add_value("address",address)
        city=response.xpath("//div[@class='ref-annonces']/span/text()").get()
        if city:
            item_loader.add_value("city",city.strip().split(" ")[0])
        zipcode=response.xpath("//div[@class='ref-annonces']/span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip().split(" ")[1])
        description=response.xpath("//div[@class='content-corps']/p/text()").get()
        if description:
            item_loader.add_value("description",description)
        room_count=response.xpath("//span[.='Nombre de pièces']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("pi")[0].strip())
        bathroom_count=response.xpath("//span[.='Nb de salle de bains']/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split("salle")[0].strip())
        deposit=response.xpath("//span[.='dépôt de garantie :']/following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0])
        utilities=response.xpath("//span[.='honoraires charge locataire TTC :']/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].split(",")[0].replace("\xa0","").strip())
        floor=response.xpath("//span[.='Etage']/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        rent=response.xpath("//span[.='Loyer CC* / mois']/following-sibling::span/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].replace("\xa0","").replace(" ","").strip())
        item_loader.add_value("currency","EUR")
        item_loader.add_value("landlord_name","Posadas Patrimoine")
        item_loader.add_value("landlord_phone","+33 1 87 21 15 37")
        item_loader.add_value("landlord_email","contact@posadas-patrimoine.com")
        yield item_loader.load_item()
