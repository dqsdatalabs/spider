# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Neueluebecker_deSpider(Spider):
    name = 'neueluebecker_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.neueluebecker.de"]
    start_urls = ["https://www.neueluebecker.de/mieten/wohnungsangebote/"]
    position = 1

    def parse(self, response):
        for url in response.css("div.block a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h2::text").get()
        if(not title):
            title = response.css("h1.csc-header-alignment-center::text").get()
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

        rent = response.css("td:contains('Netto-Kalt-Miete') + td::text").get()
        rent = rent.split(",")[0]
        if( not re.search(r"([0-9]{2,})", rent)):
            return
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"

        utilities = response.css("td:contains('Betriebskosten') + td::text").get()
        heating_cost = response.css("td:contains('Heizkosten') + td::text").get()
        water_cost = response.css("td:contains('Wasserkosten') + td::text").get()

        square_meters = response.css("td:contains('Wohnfläche') + td::text").get()
        if(not square_meters):
            square_meters = response.css("td:contains('Ladenfläche') + td::text").get()
        if(square_meters):    
            square_meters = re.findall("([0-9]+)", square_meters)
            square_meters = ".".join(square_meters)
            square_meters = math.ceil(float(square_meters))

        room_count = response.css("td:contains('Anzahl Zimmer') + td::text").get()
        room_count = math.ceil(float(room_count))

        available_date = response.css("td:contains('Verfügbar ab') + td::text").get()
        external_id = response.css("td:contains('Objektnummer') + td::text").get()

        images = response.css("img.slick-img::attr(data-lazy)").getall()
        images = [ response.urljoin(image_src) for image_src in images]
        
        address = response.css("div.adresse *::text").getall()
        address = " ".join(address)
        address = re.sub(r"\s+", " ", address)

        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        address = location_data[2]
        city = location_data[1]
        zipcode = location_data[0]

        description = response.css("div.beschreibungen p::text").getall()
        description = " ".join(description)

        description = re.sub(r'[A-Za-z0-9]*@[A-Za-z]*\.?[A-Za-z0-9]*', "", description)
        description = re.sub(r'^https?:\/\/.*[\r\n]*', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\-[0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\.[0-9]+\.[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'\([0-9]\)+ [0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r"\s+", " ", description)

        floor = response.css("td:contains('Etage') + td::text").get()

        landlord_name = "neueluebecker"
        landlord_phone = "0451 1405-0"
        landlord_email = "info@neueluebecker.de"

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

        item_loader.add_value("available_date", available_date) 

        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images)) 

        item_loader.add_value("rent_string", rent) 

        item_loader.add_value("utilities", utilities) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("water_cost", water_cost) 
        item_loader.add_value("heating_cost", heating_cost) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("floor", floor)

        yield item_loader.load_item()
