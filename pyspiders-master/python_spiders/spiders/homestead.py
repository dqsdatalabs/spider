# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
import json
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_coordinates, property_type_lookup, extract_rent_currency


class HomesteadSpider(scrapy.Spider):
    name = "homestead"
    start_urls = ['https://www.homestead.ca/apartments-for-rent/toronto']
    allowed_domains = ["homestead.ca", "lift-api.rentsync.com"]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    thousand_separator = ","
    scale_separator = "."
    position = 1

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en',
    }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.get_auth_1)

    def get_auth_1(self, response):
        client_id = response.css(".search-data::attr('data-client-id')").get()
        city_id = response.css(".search-data::attr('data-city-id')").get()
        url = f"https://{self.allowed_domains[0]}/scripts/main.js"
        yield scrapy.Request(url, callback=self.get_auth_2, meta={ 'client_id': client_id, 'city_id': city_id }, headers=self.headers)

    def get_auth_2(self, response):
        match = "&auth_token="
        left = response.text.find(match) + len(match)
        right = left + response.text[left:].find("&")
        auth_token = response.text[left:right]
        parsed = urlparse("https://lift-api.rentsync.com/v2/search")
        url_parts = list(parsed)
        query = dict(parse_qsl(url_parts[4]))
        query.update({
            'show_promotions': 'true',
            'show_custom_fields': 'true',
            'client_id': response.meta.get("client_id"),
            'city_id': response.meta.get("city_id"),
            'auth_token': auth_token,
            'min_bed': -1,
            'max_bed': 100,
            'min_bath': 0,
            'max_bath': 10,
            'min_rate': 0,
            'max_rate': 5000,
            'only_available_suites': 'true',
            'show_all_properties': 'true',
            'local_url_only': 'true',
            'keyword': 'false',
            'property_types': 'low-rise-apartment,mid-rise-apartment,high-rise-apartment,luxury-apartment,townhouse,house,multi-unit-house,single-family-home,duplex,tripex,semi',
            'order': 'featured DESC',
            'limit': 50,
            'offset': 0,
            'count': 'false',
        })
        url_parts[4] = urlencode(query)
        url = urlunparse(url_parts)
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        data = json.loads(response.body)
        for property_data in data:
            url = property_data.get("permalink")
            yield scrapy.Request(url, callback=self.extract_data, meta={ **property_data })

    def extract_data(self, response):
        external_id = str(response.meta.get("id"))
        title = response.meta.get("name")

        latitude = response.meta["geocode"]["latitude"]
        longitude = response.meta["geocode"]["longitude"]
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        square_meters = None
        area = response.meta["statistics"]["suites"]["square_feet"]
        if area["max"] and int(float(area["max"])) > 0:
            square_meters = int(float(area["max"]))
        elif area["average"] and int(float(area["average"])) > 0:
            square_meters = int(float(area["average"]))
        elif area["min"] and int(float(area["min"])) > 0:
            square_meters = int(float(area["min"]))
        if not square_meters:
            return

        pets_allowed = response.meta.get("pet_friendly")

        property_type = response.meta.get("property_type")
        for key in property_type_lookup:
            if key.lower() in property_type:
                property_type = property_type_lookup[key]

        landlord_email = response.meta["contact"]["email"]
        landlord_phone = response.meta["contact"]["phone"]
        landlord_name = response.meta["contact"]["name"]
        if not landlord_name:
            landlord_name = response.css(".brand::attr('title')").get()

        elevator = washing_machine = dishwasher = parking = balcony = swimming_pool = None
        for row in response.css("li.amenity::text").extract():
            if "Laundry" in row:
                washing_machine = True
            elif "Elevator" in row:
                elevator = True
            elif "pool" in row:
                swimming_pool = True
            elif "parking" in row:
                parking = True
            elif "Pet Friendly" in row:
                pets_allowed = True
            elif "Dishwasher" in row:
                dishwasher = True
            elif "Balconies" in row:
                balcony = True

        images = []
        for style in response.css(".cover::attr('style')").extract():
            match = "background-image:url('"
            left = style.find(match) + len(match)
            right = left + style[left:].find("'")
            images.append(style[left:right])

        description = response.css(".page-content p::text").get()
        if not description:
            description = response.css(".page-content font::text").get()

        data = {
            'external_id': external_id,
            'external_link': response.url,
            'title': title,
            'latitude': latitude,
            'longitude': longitude,
            'zipcode': zipcode,
            'city': city,
            'address': address,
            'square_meters': square_meters,
            'pets_allowed': pets_allowed,
            'property_type': property_type,
            'landlord_email': landlord_email,
            'landlord_phone': landlord_phone,
            'landlord_name': landlord_name,
            'elevator': elevator,
            'washing_machine': washing_machine,
            'dishwasher': dishwasher,
            'parking': parking,
            'balcony': balcony,
            'swimming_pool': swimming_pool,
            'images': images,
            'description': description,
        }

        for i, suite in enumerate(response.css(".suite-row")):
            room_count = suite.css(".suite-type-container .value::text").get()
            bathroom_count = round(float(suite.css(".suite-bath-container .value::text").get()))
            rent, currency = extract_rent_currency(
                suite.css(".suite-rate-container > .value > .value::text").get(),
                self.country,
                HomesteadSpider,
            )
            currency = currency.replace("USD", "CAD")
            floor_plan_images = suite.css(".floorplan-link::attr('href')").extract()

            if "Bachelor" in room_count or "One" in room_count:
                room_count = 1
            elif "Two" in room_count:
                room_count = 2
            elif "Three" in room_count:
                room_count = 3
            elif "Four" in room_count:
                room_count = 4
            elif "Five" in room_count:
                room_count = 5
            else:
                room_count = 0

            item_loader = ListingLoader(response=response)
            self.populate_item(item_loader, {
                **data, 'room_count': room_count, 'bathroom_count': bathroom_count, 'index': i+1,
                'rent': rent, 'currency': currency, 'floor_plan_images': floor_plan_images,
            })
            self.position += 1
            yield item_loader.load_item()

    def populate_item(self, item_loader, data):

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", data.get("external_link") + f"#{data.get('index')}")
        item_loader.add_value("external_id", data.get("external_id"))

        item_loader.add_value("title", data.get("title"))
        item_loader.add_value("city", data.get("city"))
        item_loader.add_value("zipcode", data.get("zipcode"))
        item_loader.add_value("address", data.get("address"))
        item_loader.add_value("latitude", data.get("latitude"))
        item_loader.add_value("longitude", data.get("longitude"))
        item_loader.add_value("property_type", data.get("property_type"))
        item_loader.add_value("square_meters", data.get("square_meters"))
        item_loader.add_value("room_count", data.get("room_count"))
        item_loader.add_value("bathroom_count", data.get("bathroom_count"))

        item_loader.add_value("pets_allowed", data.get("pets_allowed"))
        item_loader.add_value("parking", data.get("parking"))
        item_loader.add_value("elevator", data.get("elevator"))
        item_loader.add_value("balcony", data.get("balcony"))
        item_loader.add_value("swimming_pool", data.get("swimming_pool"))
        item_loader.add_value("washing_machine", data.get("washing_machine"))
        item_loader.add_value("dishwasher", data.get("dishwasher"))


        item_loader.add_value("rent", data.get("rent"))
        item_loader.add_value("currency", data.get("currency"))

        item_loader.add_value("landlord_name", data.get("landlord_name"))
        item_loader.add_value("landlord_phone", data.get("landlord_phone"))
        item_loader.add_value("landlord_email", data.get("landlord_email"))

        item_loader.add_value("floor_plan_images", data.get("floor_plan_images"))
        item_loader.add_value("images", data.get("images"))
        item_loader.add_value("description", data.get("description"))
