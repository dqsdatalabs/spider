# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Wohnbauprenzlau_deSpider(Spider):
    name = 'wohnbauprenzlau_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.wohnbauprenzlau.de"]
    start_urls = ["https://wohnbauprenzlau.de/wohnungssuchende/wohnungsuche"]
    position = 1

    def parse(self, response):
        for url in response.css("div.facts h3 a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("li.pagination-next a::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)        

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("div.facts h3::text").get()
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
        ):
            return

        rent = response.css("td:contains('Nettokaltmiete') + td::text").get()
        rent = rent.split(",")[0]
        if( not re.search(r"([0-9]{2,})", rent)):
            return
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"

        utilities = response.css("td:contains('Nebenkosten') + td::text").get()
        heating_cost = response.css("td:contains('Heizkosten') + td::text").get()
        deposit = response.css("td:contains('Kaution') + td::text").get()

        address = response.css("div.facts p::text").get()

        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]

        images = response.css("div.item a[href='#gallery'] img::attr(src)").getall()
        
        external_id = response.css("td:contains('Objektnummer') + td::text").get()
        floor = response.css("td:contains('Etage') + td::text").get()
        room_count = response.css("td:contains('Anzahl Zimmer') + td::text").get()
        square_meters = response.css("td:contains('Wohnfläche') + td::text").get()
        square_meters = re.findall("([0-9]+)", square_meters)
        square_meters = ".".join(square_meters)
        square_meters = str(math.ceil(float(square_meters)))
        energy_label = response.css("td:contains('Wertklasse') + td::text").get()

        floor_plan_images = response.css("div.section-plan a[href='#floorplan'] img::attr(src)").getall()

        special_feature = response.css("div.section-text ul li::text").getall()
        special_feature = " ".join(special_feature)

        balcony = "Balkon" in special_feature
        elevator = "Aufzug" in special_feature

        description = response.css("div.section-head:contains('Objektbeschreibung') + div.section-text::text").getall()
        description = " ".join(description).strip()
        if(description == ""):
            description = response.css("div.section-head:contains('Lage') + div.section-text::text").getall()
            description = " ".join(description).strip()


        description = re.sub(r'[A-Za-z0-9]*@[A-Za-z]*\.?[A-Za-z0-9]*', "", description, flags=re.MULTILINE)
        description = re.sub(r'^https?:\/\/.*[\r\n]*', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\-[0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\.[0-9]+\.[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'\([0-9]+\) [0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'www\.[a-z]*-?[a-z]+\.[a-z]{2,}', '', description, flags=re.MULTILINE)
        description = re.sub(r"\s+", " ", description)


        landlord_name = "wohnbauprenzlau"
        landlord_phone = " 03984 8557-77"   

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

        item_loader.add_value("elevator", elevator) 
        item_loader.add_value("balcony", balcony) 

        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images)) 
        item_loader.add_value("floor_plan_images", floor_plan_images) 

        item_loader.add_value("rent_string", rent) 
        item_loader.add_value("deposit", deposit) 
        item_loader.add_value("utilities", utilities) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("heating_cost", heating_cost) 

        item_loader.add_value("energy_label", energy_label) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 

        yield item_loader.load_item()
