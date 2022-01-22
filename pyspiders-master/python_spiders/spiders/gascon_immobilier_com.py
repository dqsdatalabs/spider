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
    name = 'gascon_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.gascon-immobilier.com/resultat.html?transaction=location&type=appartement&ville=&surfaceMin=&surfaceMax=&prixMin=&prixMax=&ref=",
                    "https://www.gascon-immobilier.com/resultat.html?transaction=location&type=f2&ville=&surfaceMin=&surfaceMax=&prixMin=&prixMax=&ref=",
                    "https://www.gascon-immobilier.com/resultat.html?transaction=location&type=f3&ville=&surfaceMin=&surfaceMax=&prixMin=&prixMax=&ref=",
                    "https://www.gascon-immobilier.com/resultat.html?transaction=location&type=f4&ville=&surfaceMin=&surfaceMax=&prixMin=&prixMax=&ref=",
                    "https://www.gascon-immobilier.com/resultat.html?transaction=location&type=f5&ville=&surfaceMin=&surfaceMax=&prixMin=&prixMax=&ref=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.gascon-immobilier.com/resultat.html?transaction=location&type=maison&ville=&surfaceMin=&surfaceMax=&prixMin=&prixMax=&ref=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.gascon-immobilier.com/resultat.html?transaction=location&type=studio&ville=&surfaceMin=&surfaceMax=&prixMin=&prixMax=&ref=",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse, meta={'property_type': url.get('property_type')})

    def parse(self, response):

        for item in response.xpath("//a[contains(.,'Voir ce bien')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Gascon_Immobilier_PySpider_france")
        
        title = response.xpath("//h1/span[@class='titre']/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
            item_loader.add_value("address", title.split("location")[1].strip())
            item_loader.add_value("city", title.split("location")[1].strip())
        
        location = response.xpath("//script[contains(.,'address')]/text()").get()
        if location:
            address = location.split('address="')[1].split('"')[0]
            item_loader.add_value("address", address)
            for i in range(1,len(address.split(" "))):
                if address.split(" ")[i].isdigit() and address[i]!="2":
                    zipcode = address.split(" ")[i]
                    item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("city", address.split(zipcode)[1].strip())

        rent = response.xpath("//h1/span[@class='prix']/text()").get()
        if rent:
            price = rent.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", int(float(price)))
        item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//li/span[contains(.,'habitable')]/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//li/span[contains(.,'chambre')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li/span[contains(.,'pièce')]/following-sibling::span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
                
        desc = " ".join(response.xpath("//div/p[contains(.,'Description')]/following-sibling::p//text() | //div/p[contains(.,'Description')]/following-sibling::text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='diapo']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        external_id = response.xpath("//li/span[contains(.,'Référence')]/following-sibling::span/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        deposit = response.xpath("//p//text()[contains(.,'Dépôt')]").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip()
            item_loader.add_value("deposit", int(float(deposit)))
        
        utilities = response.xpath("//p//text()[contains(.,'Charges') and contains(.,'€')]").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip()
            item_loader.add_value("utilities", int(float(utilities)))
        else:
            utilities = response.xpath("//p//text()[contains(.,'État des lieux :') and contains(.,'€')]").get()
            if utilities:
                utilities = utilities.split(":")[1].split("€")[0].strip()
                item_loader.add_value("utilities", int(float(utilities)))
        energy_label = response.xpath("//img/@src[contains(.,'dpe_type=0')]").get()
        if energy_label:
            energy_label = energy_label.split("dpe_value=")[1].split("&")[0]
            item_loader.add_value("energy_label", energy_label)
        
        floor = response.xpath("//li/span[contains(.,'Etage')]/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        item_loader.add_value("landlord_name", "GASCON IMMOBILIER")
        item_loader.add_value("landlord_phone", "04 67 58 99 11")
        
        yield item_loader.load_item()