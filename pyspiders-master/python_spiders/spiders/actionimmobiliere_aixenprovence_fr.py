# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re
class MySpider(Spider):
    name = 'actionimmobiliere_aixenprovence_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Actionimmobiliere_Aixenprovence_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.actionimmobiliere-aixenprovence.fr/annonces-location-appartement.html",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.actionimmobiliere-aixenprovence.fr/annonces-location-maison.html",
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

        for item in response.xpath("//a[@class='thumbnail-link']/@href").getall():  
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2 
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
   
        external_id = response.xpath("//p/i[contains(.,'réf')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(".")[-1].strip())
        city = response.xpath("//h1//text()").get()
        if city:            
            item_loader.add_value("address", city.strip().split(",")[-1])
            item_loader.add_value("city", city.strip().split(",")[-1])
        zipcode=response.xpath("//h1//small/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split("(")[1].split(")")[0].strip())

        rent = response.xpath("//li[contains(.,'Loyer')]//text()[contains(.,'Loyer')]").get()
        if rent:
            item_loader.add_value("rent",rent.replace("\n","").strip().split("€")[0].replace("\xa0","").split(" ")[-1])
        item_loader.add_value("currency","EUR")

        description = "".join(response.xpath("//div[contains(@class,'detail-offre-descriptif')]/p/text()").getall())
        if description:
            item_loader.add_value("description", description.strip().replace("\n",""))

        rooms=response.xpath("//li[@class='detail-offre-caracteristique col-sm-4 no-padding']//text()").getall()
        if rooms:
            for i in rooms:
                if "chambres" in i.lower():
                    item_loader.add_value("room_count",i.split(" ")[0])
                if "salle de bain" in i.lower() or "salle de douche" in i.lower():
                    item_loader.add_value("bathroom_count",i.split(" ")[0])
                if "parking" in i.lower():
                    item_loader.add_value("parking",True)
     
        square_meters = response.xpath("//li[contains(.,'m²')]/i/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.split("m")[0].replace(",",".").replace("\xa0","").strip())))
    
        deposit = response.xpath("//li[contains(.,'dépôt ')]//text()[contains(.,'dépôt ')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace("\n","").strip().split("€")[0].replace("\xa0",""))

        utilities = response.xpath("//li[contains(.,'mensuel')]//text()[contains(.,'dont ')]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace("\n","").split("dont")[1].split("€")[0].replace("\xa0",""))

        energy_label = response.xpath("//h4[.='Infos pratiques']//following-sibling::p[contains(.,'DPE')]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("DPE")[-1].split("-")[0].replace("\n","").replace(")","").strip())
    
        images = [response.urljoin(x) for x in response.xpath("//meta[@property='og:image']/@content").getall()]
        if images:
            item_loader.add_value("images", images)
        script_maplat = response.xpath("//script[contains(.,'lng')]//text()").get()
        if script_maplat:
            lat=script_maplat.split("lat: Number")[-1]
            item_loader.add_value("latitude", lat.split(")")[0].split("(")[-1].strip())
        script_maplng = response.xpath("//script[contains(.,'lng')]//text()").get()
        if script_maplng:
            lng=script_maplng.split("lng: Number")[-1]
            item_loader.add_value("longitude", lng.split(")")[0].split("(")[-1].strip())

        item_loader.add_value("landlord_name", "ACTION IMMOBILIERE")
        item_loader.add_value("landlord_phone", "+33 442 91 51 91")
        item_loader.add_value("landlord_email", "actionimmobiliere-aix@orange.fr")
        yield item_loader.load_item()