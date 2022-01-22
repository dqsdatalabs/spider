# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class QuattrovaniSpider(Spider):
    name = 'saskpropertymanagement_com'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.saskpropertymanagement.com"]
    start_urls = ["https://www.saskpropertymanagement.com/rental-listings/"]

    def parse(self, response):
        for url in response.css("h3.entry-title a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)
        
        next_page = response.css("ul.pagination li a::attr(href)").getall()[-1]
        if next_page:
            yield response.follow(response.urljoin(next_page), callback=self.parse)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "apartment"
        title = response.css("h1.entry-title::text").get()

        rental_terms = response.css("h3:contains('Rental Terms') + div.list-group-item-text div.opal-row div::text").getall()
        rent = rental_terms[0]
        rent = re.findall("([0-9]+)", rent)[0]
        currency = "CAD"
        available_date = rental_terms[1]

        address = response.css("div.pull-left:contains('Address:')::text").get().split(":")[1].strip()
        city = address.split(",")[1]

        room_count = response.css("li.property-label-bedrooms span.label-content::text").get()
        bathroom_count = response.css("li.property-label-bathrooms span.label-content::text").get()

        square_meters = response.css("li.property-label-parking span.label-content::text").get()
        if( square_meters ):
            square_meters = re.findall("[0-9]+", square_meters)
            square_meters = "".join(square_meters)
            square_meters = int(int(square_meters)/ 10.764)


        description = response.css("div.entry-content p::text").getall()
        description = " ".join(description).strip()
        
        amenities = response.css("h3:contains('Amenities') + div.list-group-item-text div.opal-row div::text").getall()
        if (len(amenities) == 0):
            pets_allowed = True
        else:
            pets_allowed = False

        parking = None
        if(len(amenities) == 2):
            if("Parking" in amenities[0]):
                parking = True
        else:
            parking = False

        location_script = response.css("script:contains('google.maps')::text").get()

        latitude = re.findall("lat: (-?[0-9]+\.[0-9]+)", location_script)[0]
        longitude = re.findall("lng: (-?[0-9]+\.[0-9]+)", location_script)[0]

        images = response.css("div.home-unit-slide img::attr(src)").getall()
        
        landlord_name = "saskpropertymanagement"
        landlord_phone = " 306-993-1177"
        
        dishwasher = "dishwasher" in description.lower()
        washing_machine = "laundry" in description.lower()

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("available_date", available_date)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("square_meters", int(int(square_meters)*10.764))
        item_loader.add_value("description", description)
        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("parking", parking)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("washing_machine", washing_machine)
       
        yield item_loader.load_item()