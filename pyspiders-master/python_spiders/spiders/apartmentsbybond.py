# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
import json
import re
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_rent_currency, property_type_lookup, extract_location_from_coordinates, extract_number_only


class Apartmentsbybond_PySpider_canada_en(scrapy.Spider):
    name = "apartmentsbybond"
    start_urls = ['https://www.apartmentsbybond.com/apartments']
    allowed_domains = ["apartmentsbybond.com", "theliftsystem.com"]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing' 
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.get_auth_1)

    def get_auth_1(self, response):
        match="/scripts/main.js?d="
        left = response.text.find(match) + len(match)
        timestamp = response.text[left:left+10]
        client_id = response.css("#search_data::attr('data-client-id')").get()
        city_id = response.css("#search_data::attr('data-city-id')").get()
        yield scrapy.Request(
            f"https://www.apartmentsbybond.com/scripts/main.js?d={timestamp}",
            meta={ 'client_id': client_id, 'city_id': city_id },
            callback=self.get_auth_2,
        )

    def get_auth_2(self, response):
        auth_token = re.findall('EnhancedSearch.config.authToken="([^"]*)"', response.text)[0]
        parsed = urlparse("https://api.theliftsystem.com/v2/search")
        url_parts = list(parsed)
        query = dict(parse_qsl(url_parts[4]))
        query.update({
            'client_id': response.meta.get("client_id"),
            'city_id': response.meta.get("city_id"),
            'auth_token': auth_token,
            'min_bed': -1,
            'max_bed': 100,
            'min_bath': 0,
            'max_bath': 10,
            'min_rate': 700,
            'max_rate': 2300,
            'min_sqft': 0,
            'max_sqft': 10000,
            'only_available_suites': "true",
            'show_promotions': "true",
            'local_url_only': "true",
            'keyword': "false",
            'property_types': "apartments, houses",
            'limit': 50,
            'offset': 0,
            'count': "false",
        })
        url_parts[4] = urlencode(query)
        url = urlunparse(url_parts)
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        data = json.loads(response.body)
        for property_data in data:
            for index in range(int(property_data.get("availability_count", 0))):
                url = property_data.get("permalink")
                yield scrapy.Request(url, meta={ **property_data, 'index': index }, callback=self.populate_item, dont_filter=True)

    def populate_item(self, response):
        external_id = str(response.meta.get("id"))
        title = response.meta.get("name")

        latitude = response.meta["geocode"]["latitude"]
        longitude = response.meta["geocode"]["longitude"]
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        index = response.meta.get("index")
        room_count = rent = currency = square_meters = None
        for suite in response.css(".suite"):
            if not suite.css(".available").get():
                continue
            index -= 1
            if index > -1:
                continue
            
            room_count = int(extract_number_only(suite.css(".type-name::text").get()))
            if room_count == 0:
                room_count = int("Bachelor" in suite.css(".type-name::text").get())
            bathroom_count = int(float(response.meta["statistics"]["suites"]["bathrooms"]["max"]))
            rent, currency = extract_rent_currency(suite.css(".rate-value::text").get().strip(), "canada", Apartmentsbybond_PySpider_canada_en)
            currency = currency.replace("USD", "CAD")
            square_meters = extract_number_only(suite.css(".sq-ft::text").get())
            if not square_meters:
                square_meters = int(float(response.meta["statistics"]["suites"]["square_feet"]["max"]))
            break

        pets_allowed = response.meta.get("pet_friendly")

        property_type = response.meta.get("property_type")
        for key in property_type_lookup:
            if key.lower() in property_type:
                property_type = property_type_lookup[key]

        balcony = dishwasher = parking = washing_machine = elevator = None
        for attr in response.css(".amenity::text").extract():
            if "Balconies" in attr:
                balcony = True
            elif "Dishwasher" in attr:
                dishwasher = True
            elif "parking" in attr:
                parking = True
            elif "Laundry" in attr:
                washing_machine = True
            elif "Elevator" in attr:
                elevator = True
    
        landlord_name = response.meta["contact"]["name"]
        landlord_email = response.meta["contact"]["email"]
        landlord_phone = response.meta["contact"]["phone"]

        images = response.css(".gallery-image > a::attr('href')").extract()
        description = "\r\n".join(response.css(".page-content >  p::text").extract())

        item_loader = ListingLoader(response=response)

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("external_link", response.url + f"#{rent}")
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_value("title", title)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)

        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)

        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)

        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("dishwasher", dishwasher)

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
