# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from typing import NewType
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
    name = 'idimmo_net'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Idimmo_PySpider_france"
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
        "data[Search][prixmax]": "",
        "data[Search][prixmin]": "",
        "data[Search][surfmax]":"" ,
        "data[Search][surfmin]": "",
        "data[Search][piecesmax]": "",
        "data[Search][piecesmin]": "",
        "data[Search][NO_DOSSIER]": "",
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.idimmo.net/recherche/",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield FormRequest(item,formdata=self.formdata,
                            callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//a[@class='property-v3__link']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        if page==2 or seen:
            nextpage=f"https://www.idimmo.net/recherche/{page}"
            if nextpage:
                yield Request(nextpage, callback=self.parse,meta={"page":page+1,})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)

        title=response.xpath("//div[@class='title__content']/span/text()").get()
        if title:
            item_loader.add_value("title",title)
        
        property_type=response.xpath("//div[@class='title__content']/span/text()").get()
        if property_type:
            if "Studio" in property_type:
                item_loader.add_value("property_type","studio")
            if "Appartement" in property_type:
                item_loader.add_value("property_type","apartment")
            if "Maison" in property_type:
                item_loader.add_value("property_type","house")
            if "Parking" in property_type:
                return
        external_id=response.xpath("//span[@class='detail-3__reference-number']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        rent=response.xpath("//span[.='Loyer CC* / mois']/following-sibling::span/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].replace(" ","").strip())
        item_loader.add_value("currency","EUR")
        description=response.xpath("//div[@class='detail-3__text']/p/text()").get()
        if description:
            item_loader.add_value("description",description)
        adres=response.xpath("//span[.='Ville']/following-sibling::span/text()").get()
        if adres:
            item_loader.add_value("address",adres.replace("\n","").strip())
        zipcode=response.xpath("//span[.='Code postal']/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip())
        city=response.xpath("//span[.='Ville']/following-sibling::span/text()").get()
        if city:
            item_loader.add_value("city",city.replace("\n","").strip())
        square_meters=response.xpath("//span[.='Surface habitable (m²)']/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].split(",")[0].strip())
        room_count=response.xpath("//span[.='Nombre de pièces']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        bathroom_count=response.xpath("//span[.='Nb de salle de bains']/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        furnished=response.xpath("//span[.='Meublé']/following-sibling::span/text()").get()
        if furnished and "non" in furnished.lower():
            item_loader.add_value("furnished",False)
        if furnished and "oui" in furnished.lower():
            item_loader.add_value("furnished",True)
        elevator=response.xpath("//span[.='Ascenseur']/following-sibling::span/text()").get()
        if elevator and "non" in elevator.lower():
            item_loader.add_value("elevator",False)
        if elevator and "oui" in elevator.lower():
            item_loader.add_value("elevator",True)
        balcony=response.xpath("//span[.='Balcon']/following-sibling::span/text()").get()
        if balcony and "non" in balcony.lower():
            item_loader.add_value("balcony",False)
        if balcony and "oui" in balcony.lower():
            item_loader.add_value("balcony",True)
        terrace=response.xpath("//span[.='Terrasse']/following-sibling::span/text()").get()
        if terrace and "non" in terrace.lower():
            item_loader.add_value("terrace",False)
        if terrace and "oui" in terrace.lower():
            item_loader.add_value("terrace",True)
        floor=response.xpath("//span[.='Etage']/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        utilities=response.xpath("//span[.='Honoraires TTC charge locataire']/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].split(",")[0].strip())
        deposit=response.xpath("//span[.='Dépôt de garantie TTC']/following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0].strip())
        images=[x for x in response.xpath("//picture//img//@data-src").getall()] 
        if images:
            item_loader.add_value("images",images)



        yield item_loader.load_item()