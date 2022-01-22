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
import math
import re

class MySpider(Spider):
    name = 'agencedelaposte_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Agencedelaposte_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.agencedelaposte.fr/annonces?id_polygon=&localisation_etendu=0&visite_virtuelle=&categorie=location&type_bien=appartement&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher", "property_type": "apartment"},
            {"url": "https://www.agencedelaposte.fr/annonces?id_polygon=&localisation_etendu=0&visite_virtuelle=&categorie=location&type_bien=maison&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher", "property_type": "house"},
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
        item_loader.add_value("external_source", "Agencedelaposte_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//div[contains(@class,'head-offre-titre')]//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("external_id", "//div[@class='caracteristiques-general']//li[strong[. ='Réf. : ']]/text() | //div[@class='caracteristiques-general']//li[strong[. ='Réf. : ']]/ya-tr-span/text()")

        rent = "".join(response.xpath("//p[@class='prix with-mention']//span[@itemprop='price' or @itemprop='priceCurrency']/text()").extract())
        if rent: 
            price=rent.replace(' ','')
            item_loader.add_value("rent_string", price)

        deposit = "".join(response.xpath("//li[strong[contains(.,'Dépot de garantie')]]//text()[2] | //li[strong[contains(.,'Dépot de garantie')]]/ya-tr-span//text()").extract())
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].replace(" ",""))

        utilities = "".join(response.xpath("normalize-space(//li[strong[contains(.,'Charges')]]/text()[2] | //li[strong[contains(.,'Charges')]]/ya-tr-span/text())").extract())
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0])

        square = response.xpath("//li[strong[contains(.,'Surface totale')]]/text()").get()
        item_loader.add_value("square_meters", str(math.ceil(float(square.split("m²")[0].replace(",",".")))))
            
        item_loader.add_value( "property_type", response.meta.get("property_type"))
        item_loader.add_xpath( "zipcode", "//li[strong[contains(.,'Code postal')]]/text() | //li[strong[contains(.,'Code postal')]]/ya-tr-span/text()")

        desc = "".join(response.xpath( "//p[@itemprop ='description']/text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        if "meubl\u00e9" in desc:
            item_loader.add_value("furnished", True)
        
        images = [response.urljoin(x)for x in response.xpath("//div[@class='container-content all-width container-photoslider']//ul/li/a/@href").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_xpath( "floor", "//li[strong[contains(.,'Etage')]]/text() | //li[strong[contains(.,'Etage')]]/ya-tr-span/text()")
        item_loader.add_xpath( "room_count", "//li[strong[contains(.,'pièces')]]/text() | //li[strong[contains(.,'pièces')]]/ya-tr-span/text()")

        available_date =response.xpath("//div[@class='caracteristiques-general']//li[strong[contains(.,'Disponibilité')]]/text()[. !='immédiatement                                    ']|//div[@class='caracteristiques-general']//li[strong[contains(.,'Disponibilité')]]/ya-tr-span/text()[. !='immédiatement ']").extract_first()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        bathroom_count = response.xpath(
            "//li[strong[contains(.,'salle')]]/text() | //li[strong[contains(.,'salle')]]/ya-tr-span/text()"
            ).get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        energy_label=response.xpath("//div[@class='diagnostic_images']/p/img/@data-src").get()
        if energy_label:
            energy_label=energy_label.split("dpe/")[1].split("/")[0]
            if energy_label.isdigit():
                item_loader.add_value("energy_label", energy_label_calculate(energy_label))
            
        
        if " garage" in desc:
            item_loader.add_value("parking", True)

        parking = response.xpath("//strong[contains(.,'parking')]/following-sibling::text()").get()
        if parking:
            if int(parking.strip()) > 0:
                item_loader.add_value("parking", True)
            elif int(parking.strip()) == 0:
                item_loader.add_value("parking", False)
        else:       
            parking = response.xpath("//li[strong[contains(.,'garage')]]/text() | //li[strong[contains(.,'garage')]]/ya-tr-span/text()").get()
            if parking:
                item_loader.add_value("parking", True)

        terrace = response.xpath("//li[strong[contains(.,'Ascenseur')]]/text() | normalize-//li[strong[contains(.,'Ascenseur')]]/ya-tr-span/text()").extract()
        if terrace:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//li[strong[contains(.,'Terrasse')]]/text() | normalize-//li[strong[contains(.,'Terrasse')]]/ya-tr-span/text()").extract()
        if terrace:
            item_loader.add_value("terrace", True)

        terrace = response.xpath("//li[strong[contains(.,'Balcon')]]/text() | normalize-//li[strong[contains(.,'Balcon')]]/ya-tr-span/text()").extract()
        if terrace:
            item_loader.add_value("balcony", True)

        item_loader.add_xpath("city", "//li[strong[contains(.,'Ville')]]/text() | //li[strong[contains(.,'Ville')]]/ya-tr-span/text()")
        item_loader.add_xpath("address", "//li[strong[contains(.,'Ville')]]/text() | //li[strong[contains(.,'Ville')]]/ya-tr-span/text()")

        latlng = response.xpath("normalize-space(//div[@id='myMapLocalisation']/picture//@srcset)").extract_first()
        if latlng:
            lat = latlng.split("0|")[1].split(",")[0]
            lng = latlng.split(",")[1]
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)

        item_loader.add_value("landlord_phone", "02 38 53 71 68")
        item_loader.add_value("landlord_email", "thonon@dupraz-immobilier.com")
        item_loader.add_value("landlord_name", "Agence de la Poste")

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