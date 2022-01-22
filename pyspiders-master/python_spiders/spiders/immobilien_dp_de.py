# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from scrapy.selector import Selector
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Immobilien_dp_deSpider(Spider):
    name = 'immobilien_dp_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.immobilien-dp.de"]
    start_urls = ["https://www.immobilien-dp.de/immos/alle-angebote/?mt=rent&category=&city=&address=#immobilien"]
    position = 1

    def parse(self, response):
        for url in response.css("a.btn-primary:contains('Details')::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("a.next::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)        

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("div.row h1::text").get()
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
            or "stellplätze" in lowered_title
            or "garage" in lowered_title
            or "restaurant" in lowered_title
            or "lager" in lowered_title
            or "einzelhandel" in lowered_title
            or "sonstige" in lowered_title
            or "grundstück" in lowered_title
            or "verkauf" in lowered_title
            or "reserviert" in lowered_title
            or "ladenfläche" in lowered_title
            or "halle" in lowered_title
            or "hallen" in lowered_title
            or "geschäftshaus" in lowered_title
            or "gaststätte" in lowered_title
            or "arzt" in lowered_title
        ):
            return

        badge = response.css("span.badge::text").get()
        if(badge != "Wohnung zur Miete"):
            return
        
        information_script = response.css("script.vue-tabs::text").getall()
        information_script = " ".join(information_script)
        information_selector = Selector(text=information_script)
    
        rent = information_selector.css("span.key:contains('Miete') + span.value::text").get()
        if( not rent ):
            cold_rent = information_selector.css("span.key:contains('Kaltmiete:') + span.value::text").get()
            warm_rent = information_selector.css("span.key:contains('Warmmiete:') + span.value::text").get()
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
        if(not re.search(r"([0-9]+)",rent)):
            return

        currency = "EUR"
        
        
        utilities = information_selector.css("span.key:contains('Nebenkosten') + span.value::text").get()
        deposit = information_selector.css("span.key:contains('Kaution') + span.value::text").get()

        square_meters = information_selector.css("span.key:contains('Wohnfläche') + span.value::text").get()

        room_count = information_selector.css("span.key:contains('Zimmer') + span.value::text").get()
        try:
            room_count = re.findall("([1-9])", room_count)
            room_count = "".join(room_count)
        except:
            room_count = "1"
        
        if(not re.search(r"([1-9])", room_count)):
            room_count = "1"

        bathroom_count = information_selector.css("span.key:contains('Anzahl Badezimmer') + span.value::text").get()
        
        floor = information_selector.css("span.key:contains('Anzahl Etagen:') + span.value::text").get()
        if(not floor):
            floor = information_selector.css("span.key:contains('Etage') + span.value::text").get()

        energy_label = information_selector.css("span.key:contains('Energieeffizienzklasse') + span.value::text").get()
        if(energy_label):
            energy_label = energy_label.strip()

        city = information_selector.css("p.h4::text").getall()[-1]
        
        location_data = extract_location_from_address(city)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]
        address = f"{zipcode} {city}"

        available_date = information_selector.css("span.key:contains('verfügbar ab:') + span.value::text").get()

        description = information_selector.css("v-card-text:contains('Beschreibung') p::text").getall()
        description = " ".join(description)
        description = description_cleaner(description)

        images = response.css("div#exGallery a::attr(href)").getall()
        
        elevator = "aufzug" in description
        washing_machine = "waschmaschin" in description
        parking = "Anzahl Stellplätze" in information_script or "stellplätze" in information_script or "Stellplätze" in information_script

        external_id = response.css("span:contains('Objekt-Nr.:')::text").get()
        if(external_id):
            external_id = external_id.split(":")[1]
            
        landlord_name = response.css("div.service__box p strong::text").get()
        if (not landlord_name):
            landlord_name = "D+P Immobilien GmbH"
        
        landlord_phone = response.css("service__box p:contains('Telefon:')::text").get()
        if(landlord_phone):
            landlord_phone = landlord_phone.split(":")[1]
        else: 
            landlord_phone = "02241 / 1743 – 0"
        
        landlord_email = response.css("div.service__box a::text").get()
        if (not landlord_email):
            landlord_email = "info@immobilien-dp.de"

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
        item_loader.add_value("bathroom_count", bathroom_count) 

        item_loader.add_value("available_date", available_date) 

        item_loader.add_value("elevator", elevator) 
        item_loader.add_value("washing_machine", washing_machine) 

        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images)) 

        item_loader.add_value("rent", rent) 
        item_loader.add_value("deposit", deposit) 
        item_loader.add_value("utilities", utilities) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("energy_label", energy_label) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 
        item_loader.add_value("landlord_email", landlord_email) 

        yield item_loader.load_item()
