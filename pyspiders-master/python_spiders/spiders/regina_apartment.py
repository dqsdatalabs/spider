# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
import json
from scrapy.selector import Selector
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_coordinates, extract_number_only


class Regina_apartment_PySpider_canada_en(scrapy.Spider):
    name = "regina_apartment"
    start_urls = ['https://www.reginaapartment.ca/residential?available_suites_only=1']
    allowed_domains = ["reginaapartment.ca"]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing' 
    position = 1

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
    }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.get_auth_1, headers=self.headers)

    def get_auth_1(self, response):
        client_id = response.css(".search-data::attr('data-client-id')").get()
        city_id = response.css(".search-data::attr('data-city-id')").get()
        path = response.xpath("body/script[@src]//@src").get()
        url = f"http://{self.allowed_domains[0]}{path}"
        yield scrapy.Request(url, callback=self.get_auth_2, meta={ 'city_id': city_id, 'client_id': client_id }, headers=self.headers)

    def get_auth_2(self, response):
        client_id = response.meta.get("client_id")
        city_id = response.meta.get("city_id")
        match = "&auth_token="
        left = response.text.find(match) + len(match)
        right = left + response.text[left:].find('"')
        auth_token = response.text[left:right]
        parsed = urlparse("http://api.theliftsystem.com/v2/search")
        url_parts = list(parsed)
        query = dict(parse_qsl(url_parts[4]))
        query.update({
            'client_id': client_id, 'city_id': city_id, 'auth_token': auth_token,
            'limit': 66, 'offset': 0, 'min_bed': -1, 'max_bed': 100, 'min_bath': 0, 'max_bath': 10,
            'min_rate': 0, 'max_rate': 1000, 'min_sqft': 0, 'max_sqft': 10000,
            'locale': "en", 'property_types': "apartments, houses", 'order': "min_rate ASC", 'only_available_suites': "true",
            'show_custom_fields': "true", 'show_promotions': "true", 'keyword': "false", 'count': "false",
        })
        url_parts[4] = urlencode(query)
        url = urlunparse(url_parts)
        yield scrapy.Request(url, callback=self.parse, headers=self.headers, dont_filter=True)

    def parse(self, response):
        data = json.loads(response.body)
        for property_data in data:
            url = property_data.get("permalink").replace("/our-properties/", "/residential/")
            yield scrapy.Request(url, callback=self.extract_data, meta={ **property_data }, headers=self.headers, dont_filter=True)

    def extract_data(self, response):
        external_id = str(response.meta.get("id"))
        title = response.meta.get("name")
        latitude = response.meta["geocode"]["latitude"]
        longitude = response.meta["geocode"]["longitude"]
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        address = response.css(".header-contacts span.address::text").extract().pop().strip()

        if not zipcode:
            zipcode = response.meta["address"]["postal_code"]

        available_date = response.meta.get("min_availability_date").strip()
        if not available_date:
            available_date = None

        property_type = response.meta.get("property_type").lower()
        if "apartment" in property_type:
            property_type = 'apartment'
        elif "house" in property_type:
            property_type = 'house'

        landlord_name = response.css(".brand::attr('title')").get()
        landlord_phone = response.meta["contact"]["phone"]
        landlord_email = response.meta["contact"]["email"]
        if not landlord_email:
            landlord_email = response.meta["client"]["email"]

        description_items = []
        for item in Selector(text=response.meta["details"]["overview"]).xpath("//text()").extract():
            description_items.extend([line.strip() + "." for line in item.split(".") if line.strip()])
        description = "\r\n".join([item for item in description_items if "Call" not in item])

        parking = washing_machine = balcony = dishwasher = pets_allowed = None
        for item in response.css(".amenity-holder"):
            text = " ".join(item.css("::text").extract()).strip()
            if "Balconies" in text:
                balcony = True
            elif "Laundry" in text:
                washing_machine = True
            elif "parking" in text or "garage" in text:
                parking = True
            elif "Pet free" in text:
                pets_allowed = True
        
        area = int(float(response.meta["statistics"]["suites"]["square_feet"]["max"]))

        data = {
            'external_id': external_id, 'title': title, 'latitude': latitude, 'longitude': longitude, 'zipcode': zipcode, 'city': city,
            'address': address, 'landlord_name': landlord_name, 'landlord_email': landlord_email, 'landlord_phone': landlord_phone,
            'description': description,
            'balcony': balcony, 'dishwasher': dishwasher, 'parking': parking, 'washing_machine': washing_machine,
            'pets_allowed': pets_allowed, 'available_date': available_date,
        }

        for suite in response.css(".suite-wrap"):
            square_meters = int(extract_number_only(suite.css(".suite-sqft .value::text").get()))
            if not square_meters:
                square_meters = area
            if not square_meters:
                square_meters = None

            rent = int(extract_number_only(suite.css(".suite-rate .value::text").get()))
            deposit = None
            for line in description_items:
                if "deposit" in line.lower():
                    extracted_number = int(extract_number_only(line))
                    if 0 < extracted_number <= 24:
                        deposit = rent * extracted_number
                    elif extracted_number > 24:
                        deposit = extracted_number

            unit_type = suite.css(".suite-type::text").get()
            room_count = 0
            if "Bachelor" in unit_type:
                property_type = "studio"
                room_count = 1
            elif "One" in unit_type:
                room_count = 1
            elif "Two" in unit_type:
                room_count = 2
            elif "Three" in unit_type:
                room_count = 3

            custom_data = {}
            custom_data['square_meters'] = square_meters
            custom_data['room_count'] = room_count
            custom_data['property_type'] = property_type
            custom_data['rent'] = rent
            custom_data['deposit'] = deposit
            custom_data['bathroom_count'] = round(float(extract_number_only(suite.css(".suite-bath .value::text").get())))
            custom_data['images'] = suite.css("a.suite-photo::attr('href')").extract()
            custom_data['external_link'] = response.url + "#" + str(self.position)

            item_loader = ListingLoader(response=response)
            self.populate_item(item_loader, { **data, **custom_data })
            self.position += 1
            yield item_loader.load_item()

    def populate_item(self, item_loader, data):
        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", data.get("external_link"))
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
        item_loader.add_value("washing_machine", data.get("washing_machine"))
        item_loader.add_value("dishwasher", data.get("dishwasher"))

        item_loader.add_value("rent", data.get("rent"))
        item_loader.add_value("deposit", data.get("deposit"))
        item_loader.add_value("currency", "CAD")

        item_loader.add_value("landlord_name", data.get("landlord_name"))
        item_loader.add_value("landlord_phone", data.get("landlord_phone"))
        item_loader.add_value("landlord_email", data.get("landlord_email"))

        item_loader.add_value("images", data.get("images")) # Array
        item_loader.add_value("description", data.get("description"))
