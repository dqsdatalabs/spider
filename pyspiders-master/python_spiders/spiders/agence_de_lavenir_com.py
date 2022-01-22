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
    name = 'agence_de_lavenir_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):

        start_urls = [
            {
                "url" : "http://www.agence-de-lavenir.com/fr/location-maison-villa.htm",
                "property_type" : "house"
            },
            {
                "url" : "http://www.agence-de-lavenir.com/fr/location-appartement-toulon.htm",
                "property_type" : "apartment"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='bouton']/@href").extract():
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
                meta={"property_type" : response.meta.get("property_type")}
            ) 
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_xpath("title", "//h1/text()")

        item_loader.add_value("external_source", "Agencedelavenir_PySpider_"+ self.country + "_" + self.locale)

        address_detail = response.xpath("//div[@id='lieu-detail']//text()").extract_first()
        if address_detail:
            address=address_detail.split("- ")[0].strip()
            zipcode=address_detail.split("- ")[1].strip()
            if address:
                item_loader.add_value("address",address)
                item_loader.add_value("city",address)
            if zipcode:
                item_loader.add_value("zipcode",zipcode)
          
        price = response.xpath("//div[@id='prix-immobilier-detail']//text()").extract_first()
        if price:
            item_loader.add_value("rent_string", price.replace(" ","."))
        
        desc = "".join(response.xpath("//div[@id='texte-detail']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "Dépôt de garantie  :" in desc:
                deposit =  desc.split("Dépôt de garantie  :")[1].split("€")[0].replace(" ","")
                item_loader.add_value("deposit", deposit)
            if "charge" in desc:
                try:
                    utilities = desc.split("+")[1].split("charge")[0]
                    if "€" in utilities or  "euro" in utilities:
                        item_loader.add_value("utilities", utilities)
                except:pass

        room_count = response.xpath("//div[@class='champsSPEC-row']/div[contains(.,'chambre')]/following-sibling::div//text()").extract_first()
        if room_count:
            item_loader.add_xpath("room_count", room_count)
        elif not room_count:
            item_loader.add_xpath("room_count", "//div[@class='champsSPEC-row']/div[contains(.,'pièce')]/following-sibling::div//text()")

        item_loader.add_xpath("floor", "//div[@class='champsSPEC-row']/div[contains(.,'Etage')]/following-sibling::div//text()")
        item_loader.add_xpath("bathroom_count", "//div[@class='champsSPEC-row']/div[contains(.,'Nombre de salles d')]/following-sibling::div//text()")

        square = response.xpath("//div[@class='champsSPEC-row']/div[contains(.,'Surface')]/following-sibling::div//text()").extract_first()
        if square:
            square_meters =square.split("m")[0]
            item_loader.add_value("square_meters",square_meters.strip())
        
        energy = response.xpath("//div[@id='valeur-dpe']//text()").extract_first()
        if energy:
            item_loader.add_value("energy_label", energy_label_calculate(energy.split(".")[0]))
        
        elevator = response.xpath("//div[@class='champsSPEC-row']/div[contains(.,'Ascenseur')]/following-sibling::div//text()").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)
        
        furnished = response.xpath("//div[@class='champsSPEC-row']/div[contains(.,'Meublé')]/following-sibling::div//text()").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)

        parking = response.xpath("//div[@class='champsSPEC-row']/div[contains(.,'parking')]/following-sibling::div//text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)

        terrace = response.xpath("//div[@class='champsSPEC-row']/div[contains(.,'terrasse')]/following-sibling::div//text()").extract_first()
        if terrace:
            item_loader.add_value("terrace", True) 

        images = [x for x in response.xpath("//div[contains(@class,'diaporama-photo-immobilier')]//img/@src").extract()]
        if images :
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "04 94 41 27 27")
        item_loader.add_value("landlord_name", "Agence de l'Avenir")
        item_loader.add_value("landlord_email", "agencedelavenir83@gmail.com")
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