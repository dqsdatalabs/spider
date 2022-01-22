# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Paasch_immobilien_deSpider(Spider):
    name = 'paasch_immobilien_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.paasch-immobilien.de"]
    start_urls = ["https://paasch-immobilien.de/listing-category/wohnimmobilien/"]
    position = 1

    def parse(self, response):
        properties = response.css("div.listing-wrap")
        for property_to_get in properties:
            badge = property_to_get.css("span.badge::text").get()
            if(badge == "Vermietung"):
                url = property_to_get.css("span.moretag a::attr(href)").get()
                yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)        

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h1.entry-title::text").get()
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
        
        cold_rent = response.css("span.listing-details-label:contains('Kaltmiete') + span.listing-details-value::text").get()
        warm_rent = response.css("span.listing-details-label:contains('Warmmiete') + span.listing-details-value::text").get()
         
        rent = None
        if( not re.search(r"([0-9]+)", cold_rent)):
            cold_rent = "0"
        
        if( not re.search(r"([0-9]+)", warm_rent)):
            warm_rent = "0"

        cold_rent = re.findall(r"([0-9]+)", cold_rent)
        cold_rent = "".join(cold_rent)

        warm_rent = re.findall(r"([0-9]+)", warm_rent)
        warm_rent = "".join(warm_rent)
        
        cold_rent = int(cold_rent)
        warm_rent = int(warm_rent)
        if(warm_rent > cold_rent):
            rent = str(warm_rent)
        else: 
            rent = str(cold_rent)
        
        if(not rent):
            return
        
        currency = "EUR"
        
        city = title.split(" IN ")
        if(len(city) > 1):
            city = city[1].split(" ")[0]
        else:
            city = title.split(" IM ")[1].split(" ")[0]

        utilities = response.css("span.listing-details-label:contains('Nebenkosten') + span.listing-details-value::text").get()
        deposit = response.css("span.listing-details-label:contains('Kaution') + span.listing-details-value::text").get()

        external_id = response.css("div.wpsight-listing-id::text").get()

        square_meters = response.css("span.listing-details-label:contains('Wohnfläche') + span.listing-details-value::text").get()
        square_meters = re.findall(r"([0-9]+)", square_meters)
        square_meters = ".".join(square_meters)
        square_meters = math.ceil(float(square_meters))        

        room_count = response.css("span.listing-details-label:contains('Zimmer') + span.listing-details-value::text").get()
        room_count = re.findall(r"([0-9]+)", room_count)
        room_count = ".".join(room_count)
        room_count = math.ceil(float(room_count))        

        images = response.css("div.wpsight-image-slider-item a::attr(href)").getall()
        
        description = response.css("div.wpsight-listing-description p::text").getall()
        description = " ".join(description)

        description = re.sub(r'[A-Za-z0-9]*@[A-Za-z]*\.?[A-Za-z0-9]*', "", description)
        description = re.sub(r'^https?:\/\/.*[\r\n]*', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\-[0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\.[0-9]+\.[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'\([0-9]\)+ [0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r"\s+", " ", description)

        landlord_name = response.css("div.wpsight-listing-agent-name::text").get()
        landlord_phone = response.css("span.wpsight-listing-agent-phone::text").get()
        landlord_email = response.css("span.wpsight-listing-agent-phone::text").getall()[-1]

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url) 
        item_loader.add_value("external_source", self.external_source) 

        item_loader.add_value("external_id", external_id) 
        item_loader.add_value("position", self.position) 
        self.position += 1
        item_loader.add_value("title", title) 
        item_loader.add_value("description", description) 

        item_loader.add_value("city", city) 

        item_loader.add_value("property_type", property_type)  
        item_loader.add_value("square_meters", square_meters) 
        item_loader.add_value("room_count", room_count) 

        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images)) 

        item_loader.add_value("rent_string", rent) 
        item_loader.add_value("deposit", deposit) 
        item_loader.add_value("utilities", utilities) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 
        item_loader.add_value("landlord_email", landlord_email) 

        yield item_loader.load_item()
