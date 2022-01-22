# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Greif_contzen_deSpider(Spider):
    name = 'greif_contzen_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.greif-contzen.de"]
    start_urls = ["https://www.greif-contzen.de/immobiliensuche.html#filter-results"]
    position = 1

    def parse(self, response):
        for url in response.css("div.property-teaser__right a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("li.next a::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)        

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h1.headline--primary::text").get()
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

        rent = response.css("p:contains('Kaltmiete')::text").get()
        if(rent):
            rent = rent.split(",")[0]
            if( not re.search(r"([0-9]{2,})", rent)):
                return
            rent = re.findall("([0-9]+)", rent)
            rent = "".join(rent)
        else:
            return
        currency = "EUR"
        
        utilities = response.css("p:contains('Nebenkosten')::text").get()
        if(utilities):
            utilities = utilities.split(",")[0]
            if( not re.search(r"([0-9]{2,})", utilities)):
                return
            utilities = re.findall("([0-9]+)", utilities)
            utilities = "".join(utilities)

        deposit = response.css("p:contains('Kaution')::text").get()
        if(deposit):
            deposit = deposit.split(",")[0]
            if( not re.search(r"([0-9]{2,})", deposit)):
                return
            deposit = re.findall("([0-9]+)", deposit)
            deposit = "".join(deposit)

        square_meters = response.css("p:contains('Wohnfläche')::text").get()
        if(square_meters):    
            square_meters = re.findall("([0-9]+)", square_meters)[0]
            square_meters = "".join(square_meters)

        room_count = response.css("p:contains('Anzahl Zimmer')::text").get()
        try:
            room_count = re.findall("([1-9])", room_count)
            room_count = "".join(room_count)
        except:
            room_count = "1"
        
        if(not re.search(r"([1-9])", room_count)):
            room_count = "1"
        
        city = response.css("p.bodytext--large::text").get()
        location_data = extract_location_from_address(city)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        address = location_data[2]
        city = location_data[1]
        zipcode = location_data[0]

        description = response.css("div.reveal p.bodytext::text").getall()
        description = " ".join(description)
        description = description_cleaner(description)

        images = response.css("img.slider__image::attr(src)").getall()

        images = [ response.urljoin(image_src) for image_src in images]

        external_id = response.css("p:contains('Objektnummer')::text").get()
        if(external_id):
            external_id = external_id.split("Objektnummer:")[1]

        contacts = response.css("div.teaser__textbox:contains('Ihre Ansprechpartnerin') *::text").getall()
        landlord_name = contacts[1]
        landlord_phone = response.css("a.link--tel::text").get()
        landlord_email = "welcome@greif-contzen.de"
        
        amenities = response.css("ul.list--check li::text").getall()
        amenities = " ".join(amenities)

        washing_machine = "Waschmaschine" in amenities
        elevator = "Aufzug" in amenities
        parking = "Stellplatz" in amenities

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

        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images)) 

        item_loader.add_value("elevator", elevator) 
        item_loader.add_value("washing_machine", washing_machine) 
        item_loader.add_value("parking", parking) 

        item_loader.add_value("rent", rent) 
        item_loader.add_value("deposit", deposit) 
        item_loader.add_value("utilities", utilities) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 
        item_loader.add_value("landlord_email", landlord_email) 

        yield item_loader.load_item()
