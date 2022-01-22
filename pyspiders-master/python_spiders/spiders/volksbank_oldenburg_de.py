# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Volksbank_oldenburg_deSpider(Spider):
    name = 'volksbank_oldenburg_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.volksbank-oldenburg.de"]
    start_urls = ["https://28061822-1.flowfact-webparts.net/index.php/estates?company=28061822-1&order=modified&inputMask=7889567F-16D1-4299-ADCE-05237BDDAE64"]
    position = 1

    def parse(self, response):
        for url in response.css("div.estate-details a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("div.estate-headline h1::text").get()
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

        rent = response.css("div:contains('Kaltmiete') + div::text").get()
        rent = rent.split(",")[0]
        if( not re.search(r"([0-9]{2,})", rent)):
            return
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"
        
        utilities = response.css("div:contains('Nebenkosten') + div::text").get()
        if(utilities):
            utilities = utilities.split(",")[0]

        deposit = response.css("div:contains('Kaution') + div::text").get()
        if( deposit ):
            deposit = deposit.split(",")[0]

        square_meters = response.css("div:contains('Wohnfläche') + div::text").get()
        if(square_meters):    
            square_meters = re.findall(r"([0-9]+)", square_meters)
            square_meters = ".".join(square_meters)
            square_meters = str(math.ceil(float(square_meters)))

        room_count = response.css("div.estate-list:nth-child(1) > div:nth-child(5) > div:nth-child(2)::text").get()
        try:
            room_count = re.findall(r"([1-9])", room_count)
            room_count = "".join(room_count)
        except:
            room_count = "1"
        
        if(not re.search(r"([1-9])", room_count)):
            room_count = "1"

        bathroom_count = response.css("div:contains('Anzahl Badezimmer') + div::text").get()
        balcony = response.css("div:contains('Balkone') + div::text").get()
        if(balcony):
            balcony = True
        else:
            balcony = False
        
        parking = response.css("div:contains('Stellplätze') + div::text").get()
        if(parking):
            parking = True
        else:
            parking = False

        city = response.css("div:contains('Lage') + div::text").get()
        location_data = extract_location_from_address(city)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]
        address = f"{zipcode} {city}"

        description = response.css("div.estate-description:contains('Objektbeschreibung') p::text").getall()
        description = " ".join(description)
        description = description_cleaner(description)

        images = response.css("div.slider-for-image::attr(style)").getall()
        images = [ image_src.split("background-image:url(")[1].split(")")[0] for image_src in images]

        external_id = response.css("div:contains('Objekt-ID') + td::text").get()

        contacts = response.css("div.asp-box *::text").getall()
        landlord_name = contacts[4].strip()
        landlord_email = contacts[9].strip()
        contacts = " ".join(contacts)
        contacts = re.sub(r"\s+", " ", contacts)
        landlord_phone = re.findall(r"([0-9]+)", contacts)
        landlord_phone = "".join(landlord_phone)

        energy_label = response.css("td:contains('Energieeffizienzklasse') + td::text").get()
        external_id = response.css("div:contains('Objekt-ID') + div::text").get()

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
        item_loader.add_value("bathroom_count", bathroom_count) 

        item_loader.add_value("parking", parking) 
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
        item_loader.add_value("energy_label", energy_label) 
        item_loader.add_value("external_id", external_id) 

        yield item_loader.load_item()
