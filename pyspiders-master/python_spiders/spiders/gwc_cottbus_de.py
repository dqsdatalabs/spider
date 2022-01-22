# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Gwc_cottbus_deSpider(Spider):
    name = 'gwc_cottbus_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.gwc-cottbus.de"]
    start_urls = ["https://www.gwc-cottbus.de/wohnungssuche/?x=1district=&charge_from=-&charge_to=&area_from=-&area_to=&room_from=-&room_to=&street=&attr1=&attr2=&attr3=&attr4=&attr5=&floor=&order=&by=#result"]
    position = 1

    def parse(self, response):
        for url in response.css("div.expose-result h2 a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("a:contains('weiter')::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)        

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("div.col-12 h1::text").get()
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
        if(rent):
            rent = rent.split(",")[0]
        else:
            return

        if(not re.search("([0-9]+)", rent)):
            return
        
        currency = "EUR"

        address = response.css("div.col-12 p::text").get()
        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]

        external_id = response.css("td:contains('Mietobjekt-Nr:') + td::text").get()
        room_count = response.css("td:contains('Räume') + td::text").get()
        square_meters = response.css("td:contains('Wohnfläche') + td::text").get()
        if(square_meters):
            square_meters = re.findall("([0-9]+)", square_meters)
            square_meters = ".".join(square_meters)
            square_meters = str(math.ceil(float(square_meters)))

        utilities = response.css("td:contains('Betriebskosten') + td::text").get()
        if(utilities):
            utilities = utilities.split(",")[0]
            utilities = re.findall("([0-9]+)", utilities)
            utilities = "".join(utilities)

        heating_cost = response.css("td:contains('Heizung/Warmwasser') + td::text").get()
        if(heating_cost):
            heating_cost = heating_cost.split(",")[0]
            heating_cost = re.findall("([0-9]+)", heating_cost)
            heating_cost = "".join(heating_cost)

        deposit = response.css("td:contains('Kaution') + td::text").get()
        if(deposit):
            deposit = deposit.split(",")[0]
            deposit = re.findall("([0-9]+)", deposit)
            deposit = "".join(deposit)

        amenities = response.css("h4:contains('Ausstattung') + table td::text").getall()
        amenities = " ".join(amenities)
        amenities_lowered = amenities.lower()

        elevator = "aufzug" in amenities_lowered
        balcony = "balkon" in amenities_lowered
        
        description = title + " " + amenities
        description = description_cleaner(description)

        landlord_name = response.css("div.col-lg-6:contains('Ihr/e Ansprechpartner/in:') + div.col-lg-6 strong::text").get()
        landlord_phone = response.css("div.col-lg-6:contains('Ihr/e Ansprechpartner/in:') + div.col-lg-6 span::text").get()
        landlord_email = response.css("div.col-lg-6:contains('Ihr/e Ansprechpartner/in:') + div.col-lg-6 a::text").getall()[-1]
        images = response.css("div.carousel img::attr(src)").getall()

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
        item_loader.add_value("property_type", property_type) 
        item_loader.add_value("square_meters", square_meters) 
        item_loader.add_value("room_count", room_count) 

        item_loader.add_value("elevator", elevator) 
        item_loader.add_value("balcony", balcony) 

        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images)) 

        item_loader.add_value("rent_string", rent) 
        item_loader.add_value("deposit", deposit) 
        item_loader.add_value("utilities", utilities) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("heating_cost", heating_cost) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 
        item_loader.add_value("landlord_email", landlord_email) 

        yield item_loader.load_item()
