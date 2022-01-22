# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Athing_eilers_deSpider(Spider):
    name = 'athing_eilers_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.athing-eilers.de"]
    start_urls = ["https://athing-eilers.de/mietangebote.html"]
    position = 1

    def parse(self, response):
        for url in response.css("div.details a.hyperlink_txt::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("div.title h1::text").get()
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

        rent = response.css("div.price::text").get()
        rent = rent.split(",")[0]
        if( not re.search(r"([0-9]{2,})", rent)):
            return
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"

        square_meters = response.css("div.living_space::text").getall()
        square_meters = " ".join(square_meters)
        if(square_meters):
            square_meters = square_meters.split("m²")[0]

        room_count = response.css("div.rooms::text").get()
        try:
            room_count = re.findall("([1-9])", room_count)
            room_count = "".join(room_count)
        except:
            room_count = "1"
        
        if(not re.search(r"([1-9])", room_count)):
            room_count = "1"
        
        bathroom_count = response.css("div.bathrooms::text").get()
        try:
            bathroom_count = re.findall("([1-9])", bathroom_count)
            bathroom_count = "".join(bathroom_count)
        except:
            bathroom_count = "1"
        
        if(not re.search(r"([1-9])", bathroom_count)):
            bathroom_count = "1"

        city = title.split(" in ")
        if(len(city) > 0):
            city = city[0]

        parking = response.css("li.parkplatz::text").get()
        if(parking):
            parking = True
        else: 
            parking = False
        
        balcony = "Balkon" in title

        energy_label = response.css("div:contains('Energieeffizienzklasse')::text").getall()
        energy_label = " ".join(energy_label)
        energy_label = re.findall(r"Energieeffizienzklasse:\s+([A-Z])\s*", energy_label)
        if(len(energy_label) > 0):
            energy_label = energy_label[0]

        description = response.css("div.description p::text").getall()
        description = " ".join(description)
        description = description_cleaner( description )

        images = response.css("figure.image_container a::attr(href)").getall()
        images = [ response.urljoin(image_src) for image_src in images]
        
        external_id = response.css("div.number::text").get()

        landlord_name = response.css("div.contact div.name strong::text").get()
        landlord_phone = "04488 529590"
        landlord_email = response.css("div.contact div.email a::text").get()

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        self.position += 1
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        item_loader.add_value("city", city) # String

        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean

        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int

        item_loader.add_value("rent", rent) # Int

        item_loader.add_value("currency", currency) # String

        item_loader.add_value("energy_label", energy_label) # String

        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
