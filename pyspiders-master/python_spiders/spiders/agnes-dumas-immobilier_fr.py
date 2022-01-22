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
    name = 'agnes-dumas-immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="AgnesDumasImmobilier_PySpider_france"
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    } 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.agnes-dumas-immobilier.fr/a-louer/1",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//ul[@class='listingUL']//h1[@itemprop='name']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        dontallow=response.url
        if dontallow=="http://www.agnes-dumas-immobilier.fr":
            return
        
        title = response.xpath("//div[@class='bienTitle']/h2/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ',title.strip()))

        property_type=response.xpath("//div[@class='bienTitle']/h2/text()").get()
        if property_type:
            if "Villa"==property_type:
                item_loader.add_value("property_type","house")
            if "appartement"==property_type.lower():
                item_loader.add_value("property_type","apartment")
        
        adres=response.xpath("//div[@class='themTitle']/h1/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        zipcode=response.xpath("//span[.='Code postal']/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip())
        city=response.xpath("//span[.='Ville']/following-sibling::span/text()").get()
        if city:
            item_loader.add_value("city",city.replace("\n","").strip())
        square_meters=response.xpath("//span[.='Surface habitable (m²)']/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip())
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
        description=response.xpath("//p[@itemprop='description']/text()").get()
        if description:
            item_loader.add_value("description",description)
        rent=response.xpath("//span[.='Loyer CC* / mois']/following-sibling::span/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip())
        item_loader.add_value("currency","EUR")
        utilities=response.xpath("//span[.='Honoraires TTC charge locataire']/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].strip())
        deposit=response.xpath("//span[.='Dépôt de garantie TTC']/following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0].strip())
        parking=response.xpath("//span[.='Nombre de parking']/following-sibling::span/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        images=[x for x in response.xpath("//ul//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","Agnes-Dumas-Immobilier")
        item_loader.add_value("landlord_phone","06 48 24 39 79")
        item_loader.add_value("landlord_email","contact@agnes-dumas-immobilier.fr")
        yield item_loader.load_item()    