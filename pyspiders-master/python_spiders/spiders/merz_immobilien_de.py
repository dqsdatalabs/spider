# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Merz_immobilien_deSpider(Spider):
    name = 'merz_immobilien_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.merz-immobilien.de"]
    start_urls = [
        "https://smartsite2.myonoffice.de/kunden/merzimmobiliengmbh/29/immobilien.xhtml?p[obj0]=1",
        "https://smartsite2.myonoffice.de/kunden/merzimmobiliengmbh/29/immobilien.xhtml?p[obj0]=2",
        "https://smartsite2.myonoffice.de/kunden/merzimmobiliengmbh/29/immobilien.xhtml?p[obj0]=3",
        "https://smartsite2.myonoffice.de/kunden/merzimmobiliengmbh/29/immobilien.xhtml?p[obj0]=4",
    ]
    position = 1

    def parse(self, response):
        for url in response.css("div.obj-title a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)      

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("div.shortinfo h1::text").get()
        lowered_title = title.lower()
        if(
            "gewerbe" in lowered_title
            or "gewerbefläche" in lowered_title
            or "büro" in lowered_title
            or "praxisflächen" in lowered_title
            or "ladenlokal" in lowered_title
            or "arbeiten" in lowered_title 
            or "gewerbeeinheit" in lowered_title
            or "vermietet" in lowered_title
            or "stellplatz" in lowered_title
            or "garage" in lowered_title
            or "restaurant" in lowered_title
            or "lager" in lowered_title
            or "einzelhandel" in lowered_title
            or "sonstige" in lowered_title
            or "grundstück" in lowered_title
            or "verkauf" in lowered_title
            or "reserviert" in lowered_title
        ):
            return
        type_of_use = response.css("td:contains('Nutzungsart') + td span::text").get()
        lowered_type_of_use = type_of_use.lower()
        if(
            "gewerbe" in lowered_type_of_use
            or "gewerbefläche" in lowered_type_of_use
            or "büro" in lowered_type_of_use
            or "praxisflächen" in lowered_type_of_use
            or "ladenlokal" in lowered_type_of_use
            or "arbeiten" in lowered_type_of_use 
            or "gewerbeeinheit" in lowered_type_of_use
            or "vermietet" in lowered_type_of_use
            or "stellplatz" in lowered_type_of_use
            or "garage" in lowered_type_of_use
            or "restaurant" in lowered_type_of_use
            or "lager" in lowered_type_of_use
            or "einzelhandel" in lowered_type_of_use
            or "sonstige" in lowered_type_of_use
            or "grundstück" in lowered_type_of_use
            or "verkauf" in lowered_type_of_use
            or "reserviert" in lowered_type_of_use
        ):
            return
        ad_type = response.css("td:contains('Vermarktungsart') + td span::text").get()

        if(ad_type != "Miete"):
            return
        
        cold_rent = response.css("td:contains('Kaltmiete') + td span::text").get()
        warm_rent = response.css("td:contains('Warmmiete') + td span::text").get()
        
        rent = None
        if( not cold_rent ):
            cold_rent = "0"
        
        if( not warm_rent):
            warm_rent = "0"

        cold_rent = re.findall(r"([0-9]+)", cold_rent)
        cold_rent = "".join(cold_rent)

        warm_rent = re.findall(r"([0-9]+)", warm_rent)
        warm_rent = "".join(warm_rent)
        
        cold_rent = int(cold_rent)
        warm_rent = int (warm_rent)
        if(warm_rent > cold_rent):
            rent = str(warm_rent)
        else: 
            rent = str(cold_rent)
        
        if(not re.search(r"([0-9]{2,})", rent)):
            return


        currency = "EUR"
        
        utilities = response.css("td:contains('Nebenkosten') + td span::text").get()
        deposit = response.css("td:contains('Kaution') + td span::text").get()

        square_meters = response.css("td:contains('Wohnfläche') + td span::text").get()
        room_count = response.css("td:contains('Anzahl Zimmer') + td span::text").get()
        if(room_count):
            room_count = room_count.split(",")
            room_count = ".".join(room_count)
            room_count = math.ceil(float(room_count))
        else:
            room_count = "1"
        
        city = response.css("td:contains('Ort') + td span::text").get()
        zipcode = response.css("td:contains('PLZ') + td span::text").get()
        address = f"{zipcode} {city}"

        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        description = response.css("div.obj-description span::text").getall()
        description = " ".join(description)
        description = description_cleaner(description)

        images = response.css("div.fotorama div::attr(data-img)").getall()

        external_id = response.css("td:contains('ImmoNr') + td span::text").get()
        floor = response.css("td:contains('Etage') + td span::text").get()

        balcony = response.css("td:contains('Balkon') + td span::text").get()
        if(balcony == "Ja"):
            balcony = True
        else:
            balcony = False

        landlord_name = response.css("div.name strong::text").get()
        landlord_phone = response.css("div.contact-info strong:contains('Telefon:') + span::text").get()
        landlord_email = response.css("strong:contains('E-Mail:') + span a::text").get()

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url) 
        item_loader.add_value("external_source", self.external_source) 

        item_loader.add_value("external_id", external_id) 
        item_loader.add_value("position", self.position) 
        self.position += 1
        item_loader.add_value("title", title) 
        item_loader.add_value("description", description) 

        item_loader.add_value("city", city) 
        item_loader.add_value("zipcode", zipcode) 
        item_loader.add_value("address", address) 
        item_loader.add_value("latitude", latitude) 
        item_loader.add_value("longitude", longitude) 
        item_loader.add_value("floor", floor) 
        item_loader.add_value("property_type", property_type) 
        item_loader.add_value("square_meters", square_meters) 
        item_loader.add_value("room_count", room_count) 

        item_loader.add_value("balcony", balcony) 

        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images)) 

        item_loader.add_value("rent", rent) 
        item_loader.add_value("deposit", deposit) 
        item_loader.add_value("utilities", utilities) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 
        item_loader.add_value("landlord_email", landlord_email) 

        yield item_loader.load_item()
