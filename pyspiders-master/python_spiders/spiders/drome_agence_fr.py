# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider): 
    name = 'drome_agence_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Drome_Agence_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.drome-agence.fr/annonces?id_polygon=&localisation_etendu=1&visite_virtuelle=&categorie=location&type_bien=appartement&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher",
                "property_type": "apartment"
            },
	        {
                "url": "https://www.drome-agence.fr/annonces?id_polygon=&localisation_etendu=1&visite_virtuelle=&categorie=location&type_bien=maison&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher",
                "property_type": "house"
            }, 
        ]  # LEVEL 1
        
        for url in start_urls: 
            yield Request(
                url=url.get('url'),
                callback=self.parse,
                meta={'property_type': url.get('property_type')}
        )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='info-offre']//@href[contains(.,'location')]").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//div[@class='pagelinks-next']//@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        dontallow=response.xpath("//span[@class='jelix-msg-item-error']/text()").get()
        if dontallow and "disponible à la vente" in dontallow.lower():
            return 
       
        item_loader.add_xpath("title", "//div[@class='description']/h2/text()")
        zipcode = response.xpath("//li[strong[contains(.,'Code postal')]]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        address = response.xpath("//li[strong[contains(.,'Ville')]]/text()").get()
        if address:
            item_loader.add_value("city", address)
            if zipcode:
                address = address.strip()+ " - "+ zipcode.strip()
            item_loader.add_value("address", address)
   
        item_loader.add_xpath("bathroom_count", "//li[strong[contains(.,'Nb. de salle d')]]/text()")
        rent_string = " ".join(response.xpath("//li[strong[contains(.,'Loyer')]]/text()").getall())
        if rent_string:
            item_loader.add_value("rent_string", rent_string.strip())
        utilities = " ".join(response.xpath("//li[strong[contains(.,'Charges')]]/text()").getall())
        if utilities:
            item_loader.add_value("utilities", utilities.strip())
        deposit = " ".join(response.xpath("//li[strong[contains(.,'Dépot de garantie')]]/text()").getall())
        if deposit:
            item_loader.add_value("deposit", deposit)
        description = " ".join(response.xpath("//div[@class='description']//p[@itemprop='description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        external_id = response.xpath("//li[strong[.='Réf. : ']]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        floor = response.xpath("//li[strong[contains(.,'Etage')]]/text()").get()
        if floor:
            item_loader.add_value("floor",floor.strip())
        
        latitude_longitude = response.xpath("//script[contains(.,'var latitude')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude =')[1].split(';')[0]
            longitude = latitude_longitude.split('longitude =')[1].split(';')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        available_date = response.xpath("//p/text()[contains(.,'Disponible ')]").get()
        if available_date:
            if "Disponible au" in available_date:
                available_date = available_date.split("Disponible au")[1].split(".")[0].strip()
            elif "Disponible à partir du" in available_date:
                available_date = available_date.split("Disponible à partir du")[1].strip()
            else: available_date = None
            import dateparser
            if available_date:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        parking = response.xpath("//li[strong[contains(.,'Nb. garage') or contains(.,'Nb. parking ext')]]/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        elevator = response.xpath("//li[strong[contains(.,'Ascenseur')]]/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator",False)
            else:
                item_loader.add_value("elevator",True)
        terrace = response.xpath("//li[strong[contains(.,'Terrasse')]]/text()").get()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace",False)
            else:
                item_loader.add_value("terrace",True)
        balcony = response.xpath("//li[strong[contains(.,'Balcon')]]/text()").get()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony",False)
            else:
                item_loader.add_value("balcony",True)
        room_count = response.xpath("//li[strong[contains(.,'Nb. de chambres')]]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[strong[contains(.,'Nb. de pièces')]]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)       
   
        square_meters = response.xpath("//li[strong[contains(.,'Surface totale')]]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].split(",")[0].strip())  

        images = [x for x in response.xpath("//div[@id='photoslider']//li/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
   
        item_loader.add_value("landlord_name", "Drome Agence Valence")
        item_loader.add_value("landlord_phone", "04-75-56-36-00")
     
        yield item_loader.load_item()