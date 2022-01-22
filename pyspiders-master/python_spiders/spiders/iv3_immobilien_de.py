# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Iv3_immobilien_deSpider(Spider):
    name = 'iv3_immobilien_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.iv3-immobilien.de"]
    start_urls = ["https://www.iv3-immobilien.de/de/0__72_1_0__/immobilien-deutschland-miet-objekte.html"]
    position = 1

    def parse(self, response):
        for url in response.css("div.ex_wrapper_pic a.scherpe1::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h1.h1style::text").get()
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

        cold_rent = response.css("div.eze2:contains('Kaltmiete') + div.iaus3::text").get()
        if(cold_rent):
            cold_rent = cold_rent.split(",")[0]

        warm_rent = response.css("div.eze2:contains('Warmmiete') + div.iaus3::text").get()
        if(warm_rent):
            warm_rent = warm_rent.split(",")[0]

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
        if(not re.search(r"([0-9]+)",rent)):
            return
        currency = "EUR"

        square_meters = response.css("div.eze2:contains('Wohnfläche') + div.iaus3::text").get()
        if(square_meters):
            square_meters = square_meters.split("m²")[0]

        room_count = response.css("div.eze2:contains('Zimmeranzahl') + div.iaus3::text").get()
        try:
            room_count = re.findall("([1-9])", room_count)
            room_count = "".join(room_count)
        except:
            room_count = "1"
        
        if(not re.search(r"([1-9])", room_count)):
            room_count = "1"

        bathroom_count = response.css("div.eze2:contains('Anzahl Badezimmer') + div.iaus3::text").get()
        floor = response.css("div.eze2:contains('Etage') + div.iaus3::text").get()
        
        address = response.css("div.eze2:contains('Adresse:') + div.iaus3::text").getall()
        address = " ".join(address)
        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]

        available_date = response.css("div.eze2:contains('Bezugsfrei ab') + div.iaus3::text").get()
        description = response.css("div.eze2:contains('Objektbeschreibung') + div.iaus3::text").getall()
        description = " ".join(description)
        description = description_cleaner(description)

        images = response.css("div.scherpe_wrapper_pic img::attr(src)").getall()
        images = [ response.urljoin(image_src) for image_src in images]

        external_id = response.css("div.headline_2:contains('Objekt-Nr.:')::text").get()
        if(external_id):
            external_id = external_id.split("Objekt-Nr.:")[1]

        landlord_name = "iv3-immobilien"
        landlord_phone = "0821 346 34 30"
        landlord_email = "info@iv3-immobilien.de"

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        self.position += 1
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int

        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("currency", currency) # String

        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
