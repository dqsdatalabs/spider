# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import json
import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class NicorCommunityManagementSpider(Spider):
    name = 'nicorcommunitymanagement_ca'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.nicorcommunitymanagement.ca"]

    def start_requests(self):
        yield Request(url='https://api.theliftsystem.com/v2/search?client_id=376&auth_token=sswpREkUtyeYjeoahA2i&city_id=2356&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=700&max_rate=1700&min_sqft=0&max_sqft=10000&only_available_suites=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses%2C+commercial&ownership_types=&exclude_ownership_types=&order=max_rate+ASC%2C+min_rate+ASC%2C+min_bed+ASC%2C+max_bath+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
                      callback=self.parse,
                      method='GET')

    def parse(self, response):
        parsed_response_properties = json.loads(response.body)
        for property_data in parsed_response_properties:
            yield Request(url=property_data["permalink"], callback=self.populate_item, meta = {"property_data": property_data})


    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_data = response.meta.get("property_data")
        property_type = "apartment"
        title = property_data["name"]
        
        rent = property_data["statistics"]["suites"]["rates"]["average"]
        rent = int(int(float(rent)))
        currency = "CAD"

        room_count = property_data["statistics"]["suites"]["bedrooms"]["average"]
        room_count = str(int(float(room_count)))

        bathroom_count = property_data["statistics"]["suites"]["bathrooms"]["average"]
        bathroom_count = str(int(float(bathroom_count)))
        
        square_meters = property_data["statistics"]["suites"]["square_feet"]["average"]
        square_meters = int(int(float(square_meters))/10.763)

        address = property_data["address"]['address']
        city = property_data["address"]['city']
        zipcode = property_data["address"]["postal_code"]
        images = response.css("a.gallery-cb-image::attr(href)").getall()

        landlord_name = property_data['contact']['name']
        landlord_phone = property_data['contact']['email']
        landlord_email = property_data['contact']['phone']

        pets_allowed = False
        description = property_data['details']["overview"]
        description = re.sub("</?[a-z]+>", "",description)
        latitude = property_data["geocode"]["latitude"]
        longitude = property_data["geocode"]["longitude"]
        available_date = property_data["availability_status_label"]

        external_link = property_data["permalink"]
        external_id = str(property_data["id"])

        amenities = response.css("span.amenity::text").getall()
        amenities = " ".join(amenities)
        amenities = amenities.lower()
        parking = "parking" in amenities

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("square_meters", int(int(square_meters)*10.764))
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("images", images)
        item_loader.add_value("parking", parking)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("description", description)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("available_date", available_date)
       
        yield item_loader.load_item()
