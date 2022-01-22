# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Tempoflat_deSpider(Spider):
    name = 'tempoflat_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.tempoflat.de"]
    start_urls = ["https://www.tempoflat.de/furnished-apartments/"]
    position = 1

    def parse(self, response):
        last_page_number = response.css("div#pagination_control a::text").getall()[-1]
        for page_number in range(1, int(last_page_number)):
            yield Request(f"{response.url}?p={page_number}", callback = self.get_pages, dont_filter = True )
        
    def get_pages(self, response):
        for url in response.css("div.offer_description h2 a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("div#accordion_content_main h1::text").get()
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

        rent = response.css("div.ums_panel_collapse_inner em::text").get()
        if(not re.search("([0-9]+)", rent)):
            return
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"

        external_id = response.css("div.ums_panel_collapse_inner p:contains('offer number:')::text").get()
        external_id = external_id.split(" number: ")[1]

        images = response.css("link[rel='image_src']::attr(href)").getall()
        images = [ response.urljoin(image_src) for image_src in images ] 

        amenities = response.css("div#collapse_details_description_lg *::text").getall()
        amenities = " ".join(amenities)

        balcony = "balcony" in amenities
        terrace = "terrace" in amenities
        washing_machine = "washing machine" in amenities
        furnished = "furnished" in amenities

        description = response.css("div#collapse_details_general_info p::text").getall()
        description = " ".join(description)
        description = description_cleaner(description)
        
        square_meters = re.findall("([0-9]+)m²", description)
        square_meters = "".join(square_meters)

        room_count = re.findall("([1-9]) room", title)
        room_count = "".join(room_count)
        if(not re.search("([1-9])", room_count)):
            room_count = "1"

        situation_transportation = response.css("div#collapse_details_location p::text").getall()
        situation_transportation = " ".join(situation_transportation)

        parking = "parking" in situation_transportation

        latitude = response.css("img.sr_static_map::attr(data-coords-lat)").get()
        longitude = response.css("img.sr_static_map::attr(data-coords-lng)").get()

        location_data = extract_location_from_coordinates(longitude, latitude)
        address = location_data[2]
        city = location_data[1]
        zipcode = location_data[0]
        
        landlord_name = "tempoflat"
        landlord_phone = "+41 31 381 00 60"
        landlord_email = "service@tempoflat.de"

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

        item_loader.add_value("furnished", furnished) 
        item_loader.add_value("parking", parking) 
        item_loader.add_value("balcony", balcony) 
        item_loader.add_value("terrace", terrace) 

        item_loader.add_value("washing_machine", washing_machine) 

        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images)) 

        item_loader.add_value("rent_string", rent) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 
        item_loader.add_value("landlord_email", landlord_email) 

        yield item_loader.load_item()
