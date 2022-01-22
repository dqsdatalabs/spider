# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Sahle_wohnen_deSpider(Spider):
    name = 'sahle_wohnen_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.sahle-wohnen.de"]
    start_urls = [
        "https://www.sahle-wohnen.de/immobilien-vermarktungsart/miete/?post_type=immomakler_object&ort&typ",
        "https://www.sahle-wohnen.de/immobilien-vermarktungsart/miete/page/2/?post_type=immomakler_object&ort&typ#038;ort&typ",
        "https://www.sahle-wohnen.de/immobilien-vermarktungsart/miete/page/3/?post_type=immomakler_object&ort&typ#038;ort&typ",
        "https://www.sahle-wohnen.de/immobilien-vermarktungsart/miete/page/4/?post_type=immomakler_object&ort&typ#038;ort&typ"
        ]
    position = 1

    def parse(self, response):
        for url in response.css("div.btn-group a.bg-sahle-blue::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)      

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("span.breadcrumb_last::text").get()
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

        rent = response.css("div.dt:contains('Nettokaltmiete') + div.dd::text").get()
        rent = rent.split(",")[0]
        if( not re.search(r"([0-9]{2,})", rent)):
            return
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"
        
        utilities = response.css("div.dt:contains('Nebenkosten') + div.dd::text").get()
        if(utilities):
            utilities = utilities.split(",")[0]

        heating_cost = response.css("div.dt:contains('Heizkosten') + div.dd::text").get()
        if(heating_cost):
            heating_cost = heating_cost.split(",")[0]

        square_meters = response.css("div.dt:contains('Wohnfläche') + div.dd::text").get()
        if(square_meters):
            square_meters = square_meters.split(",")[0]

        room_count = response.css("div.dt:contains('Zimmer') + div.dd::text").get()
        if(room_count):
            room_count = room_count.split(",")
            room_count = ".".join(room_count)
            room_count = str(math.ceil(float(room_count)))
            
        if(not re.search(r"([1-9])", room_count)):
            room_count = "1"
        
        bathroom_count = response.css("div.dt:contains('Badezimmer') + div.dd::text").get()
        
        floor = response.css("div.dt:contains('Etage') + div.dd::text").get()

        address = response.css("h2.property-subtitle::text").get()
        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]

        external_id = response.css("div.dt:contains('Objekt ID') + div.dd::text").get()

        available_date = response.css("div.dt:contains('Verfügbar ab') + div.dd::text").get()
        energy_label = response.css("div.dt:contains('Energie­effizienz­klasse') + div.dd::text").get()
        
        description = response.css("div.panel-body p::text").getall()
        description = " ".join(description)
        description = description_cleaner(description)

        features_list = response.css("ul.list-group li.list-group-item::text").getall()
        features_list = " ".join(features_list).strip()

        balcony = "Balkon" in features_list
        elevator = "Personenaufzug" in features_list or "Aufzug" in features_list

        images = response.css("div#immomakler-galleria a::attr(href)").getall()
        
        landlord_name = response.css("div.dt:contains('Name') + div.dd span.p-name::text").get()
        landlord_phone = response.css("div.dt:contains('Tel.') + div.dd a::text").get()
        if(landlord_phone):
            if(not re.search(r"([0-9]+)", landlord_phone)):
                landlord_phone = "02571/81-0"
        else:
            landlord_phone = "02571/81-0"
        landlord_email = response.css("div.dt:contains('E-Mail Direkt') + div.dd a::text").get()

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

        item_loader.add_value("elevator", elevator) 
        item_loader.add_value("balcony", balcony) 

        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images)) 

        item_loader.add_value("rent", rent) 

        item_loader.add_value("utilities", utilities) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("heating_cost", heating_cost) 

        item_loader.add_value("energy_label", energy_label) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 
        item_loader.add_value("landlord_email", landlord_email) 

        yield item_loader.load_item()
