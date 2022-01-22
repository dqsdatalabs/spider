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
    name = 'corneille-st-marc_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="CorneilleStMarc_PySpider_france"
    custom_settings = {"HTTPCACHE_ENABLED": False}

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://corneille-st-marc.fr/resultat.php?transac=location&type=all&budget_mini=0&budget_maxi=1000000&surface_mini=0&surface_maxi=500&nb_piece=&cp=&ville=",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='btn btn_couleur1 font1  txtBtn']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        dontallow=response.url
        if dontallow and "commercial" in response.url:
            return 
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//span[contains(.,'LOYER')]/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].split(":")[1].replace(" ",""))
        item_loader.add_value("currency","EUR")
        utilities=response.xpath("//li[contains(.,'Charges')]/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].split(":")[1])
        deposit=response.xpath("//li[contains(.,'Dépot de garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0].split(":")[1])
        external_id=response.xpath("//li[contains(.,'Référence ')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[1].strip())
        room_count=response.xpath("//li[contains(.,'Nombre de pièce(s)')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(":")[1].strip())
        square_meters=response.xpath("//li[contains(.,'Surface habitable')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(":")[-1].split("m²")[0])
        adres=response.xpath("//li[contains(.,'Localisation')]/text()").get()
        if adres:
            item_loader.add_value("address",adres.split(":")[-1].strip())
        city=response.xpath("//li[contains(.,'Localisation')]/text()").get()
        if city:
            item_loader.add_value("city",city.split("(")[0])
        zipcode=response.xpath("//li[contains(.,'Localisation')]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split("(")[1].split(")")[0])
        property_type=response.xpath("//li[contains(.,'Type de bien')]/text()").get()
        if property_type:
            if "Villa" in property_type:
                item_loader.add_value("property_type","house")
            if "appartement" in property_type.lower():
                item_loader.add_value("property_type","apartment")
        description=response.xpath("//h3[@class='font1 titreDetails']/following-sibling::text()").get()
        if description:
            item_loader.add_value("description",description)
        dontallow=description 
        if dontallow and "local commercial" in dontallow:
            return 
        bathroom_count=response.xpath("//text()[contains(.,'Salle d')]/following-sibling::b/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        energy_label=response.xpath("//text()[contains(.,'Diagnostic de performance énergétique')]").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split(":")[-1].strip())
        images=[x for x in response.xpath("//li//img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","Corneille Saint Marc")
        item_loader.add_value("landlord_phone"," 04 72 02 63 93")
        item_loader.add_value("landlord_email","csm@corneille-st-marc.fr")


        
        yield item_loader.load_item()