# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from python_spiders.helper import extract_location_from_address, sq_feet_to_meters
from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class AssociatedpmSpider(Spider):
    name = 'associatedpm_ca'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.associatedpm.ca"]
    start_urls = ["https://associatedpm.ca/residential-property-listings/"]

    def parse(self, response):
        for url in response.css("div.property-unit-information-wrapper h4 a::attr(href)").getall():
            yield Request(url, callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("li.roundright a::attr(href)").get()
       

        if (next_page != response.url and next_page != None):
            yield response.follow(url=next_page, callback=self.parse, dont_filter = True)

    def populate_item(self, response):

        property_type = "apartment"
        
        title = response.css("h1.entry-title::text").get()
        rent = response.css("div.price_area::text").get()
        if( not rent):
            return

        if("," in rent):
            rent = rent.split(",")
            rent = "".join(rent)
        
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)

        currency = "CAD"

        external_id = response.css("div.listing_detail:contains('Custom ID:')::text").get()
        
        room_count = response.css("div.listing_detail:contains('Bedrooms:')::text").get()
        if(room_count):
            room_count = math.ceil(float(room_count))
        else: 
            room_count = "1"

        bathroom_count = response.css("div.listing_detail:contains('Bathrooms:')::text").get()
        if(bathroom_count):
            bathroom_count = math.ceil(float(bathroom_count))
        else:
            bathroom_count = "1"

        description = response.css("div#wpestate_property_description_section p::text").getall()
        if(len(description) == 0):
            description = response.css("div#wpestate_property_description_section div::text").getall()
    
        description = " ".join(description)
        description = re.sub("\s+", " ", description)
        description = re.sub(r'[A-Za-z0-9]*@[A-Za-z]*\.?[A-Za-z0-9]*', "", description)
        description = re.sub(r'^https?:\/\/.*[\r\n]*', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\-[0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\.[0-9]+\.[0-9]+', '', description, flags=re.MULTILINE)

        images = response.css("div.item::attr(style)").getall()
        images_to_add = []
        for image_src in images:
            image_src = image_src.split("background-image:url(")[1]
            image_src = image_src.split(")")[0]
            images_to_add.append(image_src)
        
        address = response.css("div.listing_detail:contains('Address:')::text").get()

        city = response.css("div.listing_detail:contains('City:') a::text").get()
        zipcode = response.css("div.listing_detail:contains('Zip:')::text").get()

        address = f"{address}, {zipcode}, {city}"

        deposit = response.css("div.listing_detail:contains('Security Deposit:')::text").get()
        if(deposit):
            if("." in deposit):
                deposit = deposit.split(".")[0]
        
        available_date = response.css("div.listing_detail:contains('Available from:')::text").get()

        pets_allowed = response.css("div.listing_detail:contains('Pet Policy:')::text").get()
        if( pets_allowed):
                
            if("No" in pets_allowed):
                pets_allowed = False
            else:
                pets_allowed = True


        amenities = response.css("div.listing_detail::text").getall()

        amenities = " ".join(amenities)

        dishwasher = "Dishwasher" in amenities
        balcony = "Balcony" in amenities
        parking = "Parking" in amenities
        furnished = "furnished" in title.lower()
        washing_machine = "washer & dryer" in amenities

        landlord_name = response.css("div.agent_contanct_form_sidebar h4 a::text").get()
        landlord_phone = response.css("div.agent_contanct_form_sidebar div a::text").get()

        description = re.sub(landlord_name, '', description, flags=re.MULTILINE)
        description = re.sub(landlord_phone, '', description, flags=re.MULTILINE)

        
        latitude = response.css("div#googleMap_shortcode::attr(data-cur_lat)").get()
        longitude = response.css("div#googleMap_shortcode::attr(data-cur_long)").get()

        square_meters = response.css("div.listing_detail:contains('Property Size:')::text").get()
        if(square_meters):
            square_meters = re.findall("([0-9]+)", square_meters)
            if(len(square_meters) > 0):
                square_meters = "".join(square_meters)
        else: 
            square_meters = None
        
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("description", description)
        item_loader.add_value("images", images_to_add)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("available_date", available_date)
        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("parking", parking)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("washing_machine", washing_machine)
       
        yield item_loader.load_item()
