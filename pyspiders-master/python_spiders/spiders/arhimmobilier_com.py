# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
import dateparser
import re

class MySpider(Spider):
    name = 'arhimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Arhimmobilier_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.arhimmobilier.com/annonces?id_polygon=&localisation_etendu=0&visite_virtuelle=&categorie=location&type_bien=appartement&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher", "property_type": "apartment"},
	        {"url": "https://www.arhimmobilier.com/annonces?id_polygon=&localisation_etendu=0&visite_virtuelle=&categorie=location&type_bien=maison&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@class='liste-offres']/li//h2/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Arhimmobilier_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//div[contains(@class,'head-offre-titre')]//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
       
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.xpath("normalize-space(//strong[contains(.,'Réf. : ')]/following-sibling::text())").get())

        description = "".join(response.xpath("//div[@class='description']/p[@itemprop='description']/text()").extract())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)

        available_date = "".join(response.xpath("//strong[contains(.,'Disponibilité')]/following-sibling::text()[contains(.,'/')]").extract())
        if not available_date:
            available_date = "".join(response.xpath("//div[@class='description']/p[@itemprop='description']/text()[contains(.,'Disponible') or contains(.,'DISPONIBLE')]").extract())
        if available_date:
            try:
                match = re.search(r'(\d+/\d+/\d+)',available_date)
                if match:                    
                    new_format = dateparser.parse(match.group(1).strip()).strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", new_format)
            except:
                pass
            
        
        latlng = response.xpath("//p[@class='btn-content']/a[contains(@href,'maps')]/@href").get()
        if latlng:
            lat = latlng.split("center=")[1].split("&")[0].split(",")[0].strip()
            lng = latlng.split("center=")[1].split("&")[0].split(",")[1].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)

        address = ""
        place = response.xpath("//strong[contains(.,'Département')]/following-sibling::text()").get()
        if place:
            address = address + place + ", "

        zipcode = response.xpath("//strong[contains(.,'postal')]/following-sibling::text()").get()
        if zipcode:
            address = address + zipcode + ", "
            item_loader.add_value("zipcode", zipcode)
        
        city = response.xpath("//strong[contains(.,'Ville')]/following-sibling::text()").get()
        if city:
            address = address + city
            item_loader.add_value("city", city)
        
        if address != "":
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))

        # prop_type = response.xpath("//strong[contains(.,'bien')]/following-sibling::text()").get()
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        square_meters = response.xpath("normalize-space(//strong[contains(.,'habitable')]/following-sibling::text())").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip().replace(",", ".")
        else:
            square_meters = response.xpath("normalize-space(//strong[contains(.,'Surface totale')]/following-sibling::text())").get()
            if square_meters:
                square_meters = square_meters.split("m²")[0].strip().replace(",", ".")
        item_loader.add_value("square_meters", str(math.ceil(float(square_meters))))
        
        room_count = response.xpath("//strong[contains(.,'pièces')]/following-sibling::text()").get()
        item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//strong[contains(.,'salle de bain')]/following-sibling::text()").get()
        if bathroom_count == None:
            bathroom_count = response.xpath("//strong[contains(.,'Nb. de salle d')]/following-sibling::text()").get()
        item_loader.add_value("bathroom_count", bathroom_count)

        images = [x for x in response.xpath("//div[@id='photoslider']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        price = response.xpath("//strong[contains(.,'Loyer')]/following-sibling::text()").get()
        if price:
            if " " in price:
                price = price.replace(" ", ".")
            item_loader.add_value("rent_string", price.strip())
        # item_loader.add_value("currency", "EUR")

        deposit = response.xpath("normalize-space(//strong[contains(.,'Dépot')]/following-sibling::text())").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].replace(" ","").strip())

        utilities = response.xpath("//li/strong[contains(.,'tat des lieux')]/following-sibling::text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].replace(" ","").strip())
        
        floor = response.xpath("//strong[contains(.,'Etage')]/following-sibling::text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        parking = response.xpath("//div[@class='caracteristiques-garage']//strong[contains(.,'parking') or contains(.,'garage')]/following-sibling::text()").get()
        if parking:
            if parking.lower() != "non":
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)
        
        elevator = response.xpath("//strong[contains(.,'Ascenseur')]/following-sibling::text()").get()
        if elevator:
            if elevator.lower().strip() != "non":
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
        
        terrace = response.xpath("//strong[contains(.,'Terrasse')]/following-sibling::text()").get()
        if terrace:
            if terrace.lower().strip() != "non":
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)
        
        balcony = response.xpath("//strong[contains(.,'Balcon')]/following-sibling::text()").get()
        if balcony:
            if balcony.lower().strip() != "non":
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)
        
        item_loader.add_value("landlord_name", "ARH Immobilier Réunion")
        item_loader.add_value("landlord_phone", "0262 24 24 00")
        item_loader.add_value("landlord_email", "contact@arhimmobilier.com")

        yield item_loader.load_item()