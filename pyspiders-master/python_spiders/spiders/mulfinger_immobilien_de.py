# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Mulfinger_immobilien_deSpider(Spider):
    name = 'mulfinger_immobilien_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.mulfinger-immobilien.de"]
    start_urls = [
        "https://www.mulfinger-immobilien.de/immobilien/index.php?page=1&view=index&mode=entry&lang=de",
        "https://www.mulfinger-immobilien.de/immobilien/index.php?page=2&view=index&mode=entry&lang=de"
        ]
    position = 1

    def parse(self, response):
        for url in response.css("div.openestate_listing_entry h2 a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)       

    def populate_item(self, response):
        
        property_type = "apartment"
        property_data = {}

        title = response.css("div#openestate_expose h2::text").get()
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
            or "restaurant" in lowered_title
            or "lager" in lowered_title
            or "einzelhandel" in lowered_title
            or "sonstige" in lowered_title
            or "grundstück" in lowered_title
        ):
            return

        rent = response.css("li:contains('Kaltmiete') b::text").get()
        rent = rent.split(",")[0]
        if( not re.search(r"([0-9]{2,})", rent)):
            return
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"
        
        
        utilities = response.css("li:contains('Nebenkosten') b::text").get()
        if(utilities):
            utilities = utilities.split(",")[0]
        
        deposit = response.css("li:contains('Kaution') b::text").get()
        if(deposit):
            deposit = deposit.split(",")[0]

        square_meters = response.css("li:contains('Wohnfläche') b::text").get()
        if(square_meters):
            square_meters = square_meters.split(",")[0]
       
        room_count = response.css("li:contains('Zimmerzahl') b::text").get()
        try:
            room_count = room_count.split(",")
            room_count = ".".join(room_count)
            room_count = str(math.ceil(float(room_count)))
        except:
            room_count = "1"
        
        if(not re.search(r"([1-9])", room_count)):
            room_count = "1"
        
        bathroom_count = response.css("li:contains('Anzahl Badezimmer') b::text").get()

        address = response.css("li:contains('Adresse')::text").get()
        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]

        external_id = response.url.split("id=")[-1]

        terrace = response.css("li:contains('Terrasse') b::text").get()
        if(terrace):
            terrace = True
        else:
            terrace = False

        parking = response.css("li:contains('Anzahl Stellplätze') b::text").get()
        if(parking):
            parking = True
        else:
            parking = False

        balcony = response.css("li:contains('Balkon') b::text").get()
        if(balcony == "ja"):
            balcony = True
        else:
            balcony = False
        
        floor = response.css("li:contains('Etage') b::text").get()
        
        property_data["property_type"] = property_type
        property_data["title"] = title
        property_data["rent"] = rent
        property_data["currency"] = currency
        property_data["utilities"] = utilities
        property_data["deposit"] = deposit
        property_data["square_meters"] = square_meters
        property_data["room_count"] = room_count
        property_data["bathroom_count"] = bathroom_count
        property_data["address"] = address
        property_data["city"] = city
        property_data["zipcode"] = zipcode
        property_data["latitude"] = latitude
        property_data["longitude"] = longitude
        property_data["external_id"] = external_id
        property_data["terrace"] = terrace
        property_data["parking"] = parking
        property_data["balcony"] = balcony
        property_data["floor"] = floor
        property_data["external_link"] = response.url

        description_url = response.css("li a:contains('Beschreibung')::attr(href)").get()

        yield Request(response.urljoin(description_url), callback=self.get_description, meta = {"property_data": property_data})

    def get_description(self, response):
        property_data = response.meta.get("property_data")
        
        description = response.css("div#openestate_expose_view_content p::text").getall()
        description = " ".join(description)
        description = description_cleaner(description)
        property_data["description"] = description
        images_url = response.css("li a:contains('Galerie')::attr(href)").get()
        yield Request(response.urljoin(images_url), callback=self.get_images, meta = {"property_data": property_data})


    def get_images(self, response):
        property_data = response.meta.get("property_data")
        images = response.css("div#openestate_expose_header_image a::attr(href)").getall()
        images = [ response.urljoin(image_src) for image_src in images]

        landlord_name = "mulfinger-immobilien"
        landlord_phone = "0171 / 2441686"
        landlord_email = "mulfinger-immobilien@t-online.de"

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", property_data["external_link"]) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", property_data["external_id"]) # String
        item_loader.add_value("position", self.position) # Int
        self.position += 1
        item_loader.add_value("title", property_data["title"]) # String
        item_loader.add_value("description", property_data["description"]) # String

        item_loader.add_value("city", property_data["city"]) # String
        item_loader.add_value("zipcode", property_data["zipcode"]) # String
        item_loader.add_value("address", property_data["address"]) # String
        item_loader.add_value("latitude", property_data["latitude"]) # String
        item_loader.add_value("longitude", property_data["longitude"]) # String
        item_loader.add_value("floor", property_data["floor"]) # String
        item_loader.add_value("property_type", property_data["property_type"]) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", property_data["square_meters"]) # Int
        item_loader.add_value("room_count", property_data["room_count"]) # Int
        item_loader.add_value("bathroom_count", property_data["bathroom_count"]) # Int

        item_loader.add_value("balcony", property_data["balcony"]) # Boolean
        item_loader.add_value("terrace", property_data["terrace"]) # Boolean
        item_loader.add_value("parking", property_data["parking"]) # Boolean

        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int

        item_loader.add_value("rent", property_data["rent"]) # Int
        item_loader.add_value("deposit", property_data["deposit"]) # Int
        item_loader.add_value("utilities", property_data["utilities"]) # Int
        item_loader.add_value("currency", property_data["currency"]) # String

        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
