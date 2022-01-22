# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector 
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin


class MySpider(Spider): 
    name = 'fleurotimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):

        start_urls = [
            {
                "url" : "http://www.fleurotimmobilier.com/fr/locations-biens-immobiliers.htm?_typebien=2&_prixloyerchargecomprise=&_ville_bien=&_typebase=%252%25",
                "property_type" : "house"
            },
            {
                "url" : "http://www.fleurotimmobilier.com/fr/locations-biens-immobiliers.htm?_typebien=1&_prixloyerchargecomprise=&_ville_bien=&_typebase=%252%25",
                "property_type" : "apartment"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='texte']//a[contains(.,'savoir plus')]/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        
        next_page = response.xpath("//a[@class='rampePrecSuiv']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")},
            )
           
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Fleurotimmobilier_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//div//h1/text()").extract_first()
        if title:
            item_loader.add_value("title", title)
            address=title.split("- ")[1].strip()
            if address:
                item_loader.add_value("address", address)
                item_loader.add_value("city", address)
            
        price = response.xpath("//div[@class='prix-immobilier-detail']//text()").extract_first()
        if price:
            item_loader.add_value("rent_string", price.replace(" ",""))

        external_id = response.xpath("//div[@class='description-immobilier' and contains(.,'Réf.')]/strong//text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Réf.")[1].strip())       
  
        room_count = response.xpath("//div//strong[contains(.,' chambre')]/following-sibling::text()[1]").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count )
        elif not room_count:
            room_count1=response.xpath("//div//strong[contains(.,' pièces')]/following-sibling::text()[1]").extract_first()
            if room_count1:
               item_loader.add_value("room_count", room_count1)

        bathroom_count = response.xpath("//div//strong[contains(.,' salles d')]/following-sibling::text()[1]").get()
        
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        square = response.xpath("//div//strong[contains(.,'Surface habitable')]/following-sibling::text()[1]").extract_first()
        if square:
            square_meters = int(float(square.split("m")[0].strip()))
            item_loader.add_value("square_meters", square_meters)
        
        floor = response.xpath("//div//strong[contains(.,'Etage ')]/following-sibling::text()[1]").extract_first()
        if floor:
            if floor.isdigit():
                item_loader.add_value("floor", floor)

        utilities = response.xpath("//div//strong[contains(.,' lieux charge')]/following-sibling::text()[1]").extract_first()
        if utilities:   
            item_loader.add_value("utilities", utilities.split(".")[0])

        desc = "".join(response.xpath("//div[@class='texte-liste']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "Dépôt de garantie" in desc:
                deposit = desc.split("Dépôt de garantie  :")[1].split("€")[0]
                item_loader.add_value("deposit", deposit.strip())
              
        balcony = response.xpath("//div//strong[contains(.,'balcon')]/following-sibling::text()[1]").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//div//strong[contains(.,'parking')]/following-sibling::text()[1]").extract_first()
        if parking:
            item_loader.add_value("parking", True)

        elevator = response.xpath("//div//strong[contains(.,'Ascenseur')]/following-sibling::text()[1]").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)

        furnished = response.xpath("//div//strong[contains(.,'Meublé')]/following-sibling::text()[1]").extract_first()
        if furnished:
            if furnished == "Oui":
                item_loader.add_value("furnished", True)
        
        swimming_pool = response.xpath("//div//strong[contains(.,'Piscine')]/following-sibling::text()[1]").get()
        if swimming_pool:
            if "Oui" in swimming_pool:
                item_loader.add_value("swimming_pool", True)
        
        terrace = response.xpath("//div//strong[contains(.,'terrasse') or contains(.,'Terrasse')]/following-sibling::text()[1]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        images = [x for x in response.xpath("//div[contains(@class,'diaporama-photo-immobilier')]//img/@src").extract()]
        if images :
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "04 42 08 46 59")
        item_loader.add_value("landlord_name", "Fleurot Immobilier")
        item_loader.add_value("landlord_email", "n.legrand@fleurotimmobilier.com")
        yield item_loader.load_item()

