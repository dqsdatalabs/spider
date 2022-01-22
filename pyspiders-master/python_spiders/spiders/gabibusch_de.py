# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Gabibusch_deSpider(Spider):
    name = 'gabibusch_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.gabibusch.de"]
    start_urls = ["https://gabibusch.de/immobilien-angebote/"]
    position = 1

    def parse(self, response):
        for url in response.css("a.fusion-button:contains('Zum Exposé')::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h1 strong::text").get()
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

        rent = response.css("li:contains('Kaltmiete')::text").get()
        rent = rent.split(",")[0]
        if( not re.search(r"([0-9]{2,})", rent)):
            return
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"

        utilities = response.css("li:contains('Nebenkosten')::text").get()
        utilities = utilities.split(",")[0]
        if( not re.search(r"([0-9]{2,})", utilities)):
            return
        utilities = re.findall("([0-9]+)", utilities)
        utilities = "".join(utilities)

        deposit = response.css("li:contains('Kaution')::text").get()
        deposit = deposit.split(",")[0]
        if( not re.search(r"([0-9]{2,})", deposit)):
            return
        deposit = re.findall("([0-9]+)", deposit)
        deposit = "".join(deposit)

        square_meters = response.css("li:contains('Wohnfläche')::text").get()
        if(not square_meters):
            square_meters = response.css("li:contains('Nutzfläche')::text").get()

        if(square_meters):
            square_meters = square_meters.split("m² Wohnfläche")[0]

        room_count = response.css("li:contains('Zimmer')::text").get()
        try:
            room_count = re.findall("([1-9])", room_count)
            room_count = "".join(room_count)
        except:
            room_count = "1"
        
        if(not re.search(r"([1-9])", room_count)):
            room_count = "1"
        
        bathroom_count = response.css("li:contains('Badezimmer')::text").get()
        if(bathroom_count):
            bathroom_count = "1"

        description = response.css("div.fusion-text p::text").getall()
        description = " ".join(description)
        description = description_cleaner(description)

        images = response.css("div.fusion-gallery-image a::attr(href)").getall()
        try:
            external_id = response.css("h1.entry-title::text").get()
            if(external_id):
                external_id = external_id.split("Objekt-Nr.")[1]
        except:
            external_id = response.css("h1.entry-title::text").get()
            if(external_id):
                external_id = external_id.split("Objekt Nr.")[1]        
        landlord_name = "gabibusch"
        landlord_phone = "0611 – 185 16 01"
        landlord_email = "kontakt@gabibusch.de"

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        self.position += 1
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int

        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
