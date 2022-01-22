# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import json

import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *

class WellgroundedrealestateSpider(scrapy.Spider):
    name = "ppml"
    start_urls = ['https://api.theliftsystem.com/v2/search?local_url_only=true&only_available_suites=true&show_promotions=true&client_id=173&auth_token=sswpREkUtyeYjeoahA2i&city_id=1425&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1400&max_rate=2800&region=&keyword=false&property_types=motel,+construction,+luxury-apartment,+low-rise-apartment,+mid-rise-apartment,+high-rise-apartment,+townhouse,+multi-unit-house,+single-family-home,+duplex,+triplex,+fourplex,+rooms,+semi,+house&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC,+max_rate+ASC,+min_bed+ASC,+max_bed+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false'
                  ,"https://api.theliftsystem.com/v2/search?local_url_only=true&only_available_suites=true&show_promotions=true&client_id=173&auth_token=sswpREkUtyeYjeoahA2i&city_id=1837&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=4600&region=&keyword=false&property_types=motel,+construction,+luxury-apartment,+low-rise-apartment,+mid-rise-apartment,+high-rise-apartment,+townhouse,+multi-unit-house,+single-family-home,+duplex,+triplex,+fourplex,+rooms,+semi,+house&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC,+max_rate+ASC,+min_bed+ASC,+max_bed+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false"
                  ,"https://api.theliftsystem.com/v2/search?local_url_only=true&only_available_suites=true&show_promotions=true&client_id=173&auth_token=sswpREkUtyeYjeoahA2i&city_id=2377&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=3000&region=&keyword=false&property_types=motel,+construction,+luxury-apartment,+low-rise-apartment,+mid-rise-apartment,+high-rise-apartment,+townhouse,+multi-unit-house,+single-family-home,+duplex,+triplex,+fourplex,+rooms,+semi,+house&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC,+max_rate+ASC,+min_bed+ASC,+max_bed+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false"
                  ,"https://api.theliftsystem.com/v2/search?local_url_only=true&only_available_suites=true&show_promotions=true&client_id=173&auth_token=sswpREkUtyeYjeoahA2i&city_id=3133&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=6000&region=&keyword=false&property_types=motel,+construction,+luxury-apartment,+low-rise-apartment,+mid-rise-apartment,+high-rise-apartment,+townhouse,+multi-unit-house,+single-family-home,+duplex,+triplex,+fourplex,+rooms,+semi,+house&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC,+max_rate+ASC,+min_bed+ASC,+max_bed+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false"
                  "https://api.theliftsystem.com/v2/search?local_url_only=true&only_available_suites=true&show_promotions=true&client_id=173&auth_token=sswpREkUtyeYjeoahA2i&city_id=3284&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1400&max_rate=2800&region=&keyword=false&property_types=motel,+construction,+luxury-apartment,+low-rise-apartment,+mid-rise-apartment,+high-rise-apartment,+townhouse,+multi-unit-house,+single-family-home,+duplex,+triplex,+fourplex,+rooms,+semi,+house&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC,+max_rate+ASC,+min_bed+ASC,+max_bed+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false"
                  ,"https://api.theliftsystem.com/v2/search?local_url_only=true&only_available_suites=true&show_promotions=true&client_id=173&auth_token=sswpREkUtyeYjeoahA2i&city_id=3133&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=6000&region=&keyword=false&property_types=motel,+construction,+luxury-apartment,+low-rise-apartment,+mid-rise-apartment,+high-rise-apartment,+townhouse,+multi-unit-house,+single-family-home,+duplex,+triplex,+fourplex,+rooms,+semi,+house&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC,+max_rate+ASC,+min_bed+ASC,+max_bed+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false"]
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        global pos
        resp = json.loads(response.body)
        items = resp
        for j, item in enumerate(items):
            link = item.get("permalink")
            yield scrapy.Request(link, callback=self.suites, meta={"item": item})

    def suites(self,response):
        room_count = None
        bathroom_count = None
        floor = None
        parking = None
        elevator = None
        balcony = None
        washing_machine = None
        dishwasher = None
        utilities = None
        terrace = None
        furnished = None
        property_type = None
        energy_label = None
        deposit = None
        square_meters = None
        swimming_pool = None
        external_id = None
        pets_allowed = None
        heating_cost = None
        item_loader=response.meta.get("item_loader")
        item=response.meta.get("item")
        suites=item.get("availability_count")
        pics=response.xpath('.//div[@class="image"]/img/@src').extract()
        floor_plan_images=response.xpath('.//div[@class="suite-floorplans cell"]/a/@href').extract()
        link = item.get("permalink")
        room_count=None
        rent=None
        bathroom_count=None
        square_meters=None
        images=[]
        title = item.get("name")
        link = item.get("permalink")
        prop = item.get("property_type")
        if "house" in prop.lower():
            property_type = "house"
        else:
            property_type = "apartment"
        longitude = item.get("geocode").get("longitude")
        latitude = item.get("geocode").get("latitude")
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        external_id = str(item.get("id"))
        landlord_number = item.get("client").get("phone")
        landlord_email = item.get("client").get("email")
        landlord_name = item.get("client").get("name")
        available = item.get("availability_status_label")
        for pic in pics:
            if "128" not in pic:
                images.append(pic)
            if "floor" in pic and "128" not in pic:
                floor_plan_images.append(pic)
        for i in range(suites):
            prices = "".join(response.xpath('.//div[@class="suite-rate cell"]/div/text()').extract()).replace("\n",
                                                                                                              "").replace(
                "  ", "")[1:].split("$")
            try:
                rent = int(prices[i])
            except:
                return
            room_count = int(item.get("matched_beds")[i])
            bathroom_count = int(float(item.get("matched_baths")[i]))
            try:
                square_meters = response.xpath('.//span[@class="sq-ft"]/text()').extract()[i].strip().replace(
                    " sq.ft.", "")
                square_meters = int(square_meters)
            except:
                pass
            amen = "".join(response.xpath('.//span[@class="amenity"]/text()').extract())
            description = item.get("details").get("overview")
            description = description_cleaner(description)
            description = description.replace('http://winsold.com/tour/17585', "")
            item_loader = ListingLoader(response=response)
            pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities(description, amen, item_loader)
        # # MetaData
            item_loader.add_value("external_link", response.url+f"#{i}")  # String
            item_loader.add_value("external_source", self.external_source)  # String
            item_loader.add_value("external_id", external_id)  # String
            item_loader.replace_value("position", self.position)  # Int
            item_loader.add_value("title", title)  # String
            item_loader.add_value("description", description)  # String
            # # Property Details
            item_loader.add_value("city", city)  # String
            item_loader.add_value("zipcode", zipcode)  # String
            item_loader.add_value("address", address)  # String
            item_loader.add_value("latitude", str(latitude))  # String
            item_loader.add_value("longitude", str(longitude))  # String
            item_loader.add_value("floor", floor)  # String
            item_loader.add_value("property_type",
                                  property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("available_date", available)  # String => date_format
            # item_loader.add_value("water_cost", water_cost) # Int

            item_loader.replace_value("square_meters", square_meters)  # Int
            item_loader.replace_value("room_count", room_count)  # Int
            item_loader.replace_value("bathroom_count", bathroom_count)  # Int

            item_loader.replace_value("pets_allowed", pets_allowed)  # Boolean
            item_loader.replace_value("furnished", furnished)  # Boolean
            item_loader.replace_value("parking", parking)  # Boolean
            item_loader.replace_value("elevator", elevator)  # Boolean
            item_loader.replace_value("balcony", balcony)  # Boolean
            item_loader.replace_value("terrace", terrace)  # Boolean
            item_loader.replace_value("swimming_pool", swimming_pool)  # Boolean
            item_loader.replace_value("washing_machine", washing_machine)  # Boolean
            item_loader.replace_value("dishwasher", dishwasher)  # Boolean
            item_loader.replace_value("images", images)  # Array
            item_loader.replace_value("external_images_count", len(images))  # Int
            item_loader.replace_value("floor_plan_images", floor_plan_images)  # Array
            item_loader.replace_value("rent", rent)  # Int
            item_loader.add_value("currency", "EUR")  # String

            item_loader.add_value("heating_cost", heating_cost)  # Int

            item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name)  # String
            item_loader.add_value("landlord_phone", landlord_number)  # String
            item_loader.add_value("landlord_email", landlord_email)  # String
            self.position += 1
            yield item_loader.load_item()




