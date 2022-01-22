# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Engelvoelkers_com_germanySpider(Spider):
    name = 'engelvoelkers_com_germany'
    name2 = 'engelvoelkers_com'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name2.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.engelvoelkers.com"]
    start_urls = [
        "https://www.engelvoelkers.com/de/search/?q=&startIndex=0&businessArea=residential&sortOrder=DESC&sortField=newestProfileCreationTimestamp&pageSize=18&facets=bsnssr%3Aresidential%3Bcntry%3Agermany%3Bobjcttyp%3Acondo%3Btyp%3Arent%3B",
        "https://www.engelvoelkers.com/de/search/?q=&startIndex=0&businessArea=residential&sortOrder=DESC&sortField=newestProfileCreationTimestamp&pageSize=18&facets=bsnssr%3Aresidential%3Bcntry%3Agermany%3Bobjcttyp%3Ahouse%3Btyp%3Arent%3B"
    ]
    position = 1

    def parse(self, response):
        for url in response.css("a.ev-property-container::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("a.ev-pager-next::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h1.ev-exposee-title::text").get()
        lowered_title = title.lower()
        if("veermietet" in lowered_title):
            return
        if("vermietet" in lowered_title):
            return
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
            or "office" in lowered_title
        ):
            return
        rent = response.css("div.ev-key-fact:contains('Gesamtmiete')").css("div.ev-key-fact-value::text").get()
        
        if(rent):
            if(not re.search("([0-9]+)", rent)):
                return
        else:
            return
        
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"

        room_count = response.css("div.ev-key-fact:contains('Zimmer')").css("div.ev-key-fact-value::text").get()
        if(not room_count):
            room_count = response.css("div.ev-key-fact:contains('Schlafzimmer')").css("div.ev-key-fact-value::text").get()
        if( room_count):
            if("," in room_count):
                room_count = room_count.split(",")
                room_count = ".".join(room_count)
            room_count = math.ceil(float(room_count))
        else:
            room_count = "1"
        
        bathroom_count = response.css("div.ev-key-fact:contains('Badezimmer')").css("div.ev-key-fact-value::text").get()
        
        square_meters = response.css("div.ev-key-fact:contains('Wohnfläche')").css("div.ev-key-fact-value::text").get()
        if not square_meters: 
            square_meters = response.css("div.ev-key-fact:contains('Gesamtfl')").css("div.ev-key-fact-value::text").get()
        if not square_meters: 
            square_meters = response.css("div.ev-key-fact:contains('Grundstück')").css("div.ev-key-fact-value::text").get()

        if(square_meters):
            if("." in square_meters):
                square_meters = square_meters.split(".")
                square_meters = ''.join(square_meters)

        utilities = response.css("span.ev-exposee-detail-fact-value:contains('Nebenkosten')::text").get()
        if(utilities):
            utilities = utilities.split(",")[0]
            utilities = re.findall("([0-9]+)", utilities)
            utilities = "".join(utilities)
            if(not re.search("([0-9]+)", utilities)):
                utilities = None

        external_id = response.css("li.ev-exposee-detail-fact:contains('ID') span.ev-exposee-detail-fact-value::text").get()
        images = response.css("a.ev-image-gallery-image-link img.ev-image-gallery-image::attr(src)").getall()

        amenities = response.css("ul.ev-exposee-content li.ev-exposee-detail-fact span.ev-exposee-detail-fact-value::text").getall()

        amenities = " ".join(amenities)

        elevator = "Aufzug" in amenities
        terrace = "Terrasse " in amenities
        balcony = "Balkon" in amenities

        description = response.css("p.ev-exposee-text::text").getall()
        description = " ".join(description)

        floor_plan_images = response.css("a.ev-only-one img.ev-image-gallery-image::attr(src)").getall()

        address = response.css("div.ev-exposee-subtitle::text").get()
        if(address):
            address = address.split("|")[1]

        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])
        
        location_data = extract_location_from_coordinates(longitude, latitude)
        address = location_data[2]
        city = location_data[1]
        if(not city):
            city = title.split(" in ")[1]
        zipcode = location_data[0]

        landlord_name = "engelvoelkers"
        landlord_phone = response.css("span[itemprop='telephone']::text").get()

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

        item_loader.add_value("elevator", elevator) 
        item_loader.add_value("balcony", balcony) 
        item_loader.add_value("terrace", terrace) 

        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images)) 
        item_loader.add_value("floor_plan_images", floor_plan_images)

        item_loader.add_value("rent_string", rent) 
        item_loader.add_value("utilities", utilities) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 

        yield item_loader.load_item()
