# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Matheisen_immobilien_deSpider(Spider):
    name = 'matheisen_immobilien_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.matheisen-immobilien.de"]
    start_urls = ["https://www.matheisen-immobilien.de/Alle-Angebote.htm"]
    position = 1

    def parse(self, response):
        for url in response.css("div.details a.gotoLink::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("div.objektTitel::text").get()
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

        rent = response.css("div.tableLabel:contains('Kaltmiete') + div.tableValue::text").get()
        rent = rent.split(",")[0]
        if( not re.search(r"([0-9]{2,})", rent)):
            return
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"
        
        object_type = response.css("div.tableLabel:contains('Objektart') + div.tableValue::text").get()
        if( object_type != "Wohnung"):
            return
        
        utilities = response.css("div.tableLabel:contains('Nebenkosten') + div.tableValue::text").get()
        if(utilities):
            utilities = utilities.split(",")[0]

        deposit = response.css("div.tableLabel:contains('Kaution') + div.tableValue::text").get()
        if(deposit):
            deposit = deposit.split(",")[0]

        square_meters = response.css("div.tableLabel:contains('Wohnfläche') + div.tableValue::text").get()
        if(square_meters):
            square_meters = square_meters.split(",")[0]
        

        room_count = response.css("div.tableLabel:contains('Zimmeranzahl') + div.tableValue::text").get()
        room_count = room_count.split(",")
        room_count = ".".join(room_count)
        room_count = str(math.ceil(float(room_count)))
    
        if(not re.search(r"([1-9])", room_count)):
            room_count = "1"

        floor = response.css("div.tableLabel:contains('Etage') + div.tableValue::text").get()
        
        city = response.css("div.tableLabel:contains('Ort') + div.tableValue::text").get()
        location_data = extract_location_from_address(city)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        address = location_data[2]
        city = location_data[1]
        zipcode = location_data[0]

        energy_label = response.css("div.tableLabel:contains('Energieeffizienzklasse') + div.tableValue::text").get()
        
        description = response.css("div.objektTextElement div.text::text").getall()
        description = " ".join(description)
        description = description_cleaner(description)
        
        images = response.css("div.hiddenLinks a.venobox::attr(href)").getall()

        images = [ response.urljoin(image_src) for image_src in images]
        external_id = response.css("span.objNrClass::text").get()
        external_id = external_id.split("ID:")[1]

        landlord_name = response.css("div.personName::text").get()
        landlord_phone = response.css("div.text_tel::text").get()
        landlord_phone = landlord_phone.split(":")[1]
        landlord_email = response.css("div.text_email a::attr(href)").get()
        landlord_email = landlord_email.split(":")[1]

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
