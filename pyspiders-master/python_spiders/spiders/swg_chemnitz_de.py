# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Nib_deSpider(Spider):
    name = 'swg_chemnitz_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.swg-chemnitz.de"]
    start_urls = ["https://www.swg-chemnitz.de/mieten/"]
    position = 1

    def parse(self, response):
        for url in response.css("h3.property-title a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("a.next::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)        

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h1.property-title::text").get()
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

        rent = response.css("div.dt:contains('Kaltmiete') + div.dd::text").get()
        rent = rent.split(",")[0]
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        if(not re.search("([0-9]+)", rent)):
            return
        currency = "EUR"

        address = response.css("div.dt:contains('Adresse') + div.dd::text").getall()
        address = " ".join(address)
        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])
        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]

        external_id = response.css("div.dt:contains('Objekt ID') + div.dd::text").get()
        floor = response.css("div.dt:contains('Etage') + div.dd::text").get()
        square_meters = response.css("div.dt:contains('Wohnfläche') + div.dd::text").get()
        square_meters = square_meters.split(",")[0]

        room_count = response.css("div.dt:contains('Zimmer') + div.dd::text").get()
        bathroom_count = response.css("div.dt:contains('Badezimmer') + div.dd::text").get()
        available_date = response.css("div.dt:contains('Verfügbar ab') + div.dd::text").get()
        deposit = response.css("div.dt:contains('Kaution') + div.dd::text").get()
        if(deposit != None):
            deposit = deposit.split(",")[0]
            deposit = re.findall("([0-9]+)", deposit)
            deposit = "".join(deposit)

        heating_cost = response.css("div.dt:contains('Heizkosten') + div.dd::text").get()
        if(heating_cost != None):
            heating_cost = heating_cost.split(",")[0]
            heating_cost = re.findall("([0-9]+)", heating_cost)
            heating_cost = "".join(heating_cost)

        utilities = response.css("div.dt:contains('Nebenkosten') + div.dd::text").get()
        if(utilities):
            utilities = utilities.split(",")[0]
            utilities = re.findall("([0-9]+)", utilities)
            utilities = "".join(utilities)

        amenities = response.css("li.list-group-item::text").getall()
        amenities = " ".join(amenities).strip()

        terrace = "Terrasse" in amenities
        balcony = "Balkon" in amenities

        energy_label = response.css("div.dt:contains('Energie­effizienz­klasse') + div.dd::text").get()
        
        images = response.css("div#immomakler-galleria a::attr(href)").getall()
        
        description = response.css("div.panel-body p::text").getall()
        description = " ".join(description)

        description = re.sub(r'[A-Za-z0-9]*@[A-Za-z]*\.?[A-Za-z0-9]*', "", description, flags=re.MULTILINE)
        description = re.sub(r'^https?:\/\/.*[\r\n]*', '', description, flags=re.MULTILINE)
        description = re.sub(r'www\.[a-z]*-?[a-z]+\.[a-z]{2,}', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\-[0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\.[0-9]+\.[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'\([0-9]+\) [0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r"\s+", " ", description)

        landlord_name = "Sächsische Wohnungsgenossenschaft Chemnitz eG"
        landlord_phone = "0371 44440 0"
        landlord_email = "info@swg-chemnitz.de"

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

        item_loader.add_value("balcony", balcony) 
        item_loader.add_value("terrace", terrace) 

        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images)) 

        item_loader.add_value("rent_string", rent) 
        item_loader.add_value("deposit", deposit) 
        item_loader.add_value("utilities", utilities) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("heating_cost", heating_cost) 

        item_loader.add_value("energy_label", energy_label) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 
        item_loader.add_value("landlord_email", landlord_email) 

        yield item_loader.load_item()
