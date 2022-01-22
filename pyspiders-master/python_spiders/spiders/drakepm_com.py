# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import scrapy
import json
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re


class DrakepmSpider(scrapy.Spider):

    name = "drakepm"
    
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        url = f'https://api.theliftsystem.com/v2/search?client_id=432&auth_token=sswpREkUtyeYjeoahA2i&city_ids=1174,329&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1500&max_rate=2000&min_sqft=0&max_sqft=10000&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=max_rate+ASC,+min_rate+ASC,+min_bed+ASC,+max_bath+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false'
        yield Request(url, callback=self.parse)
    # 2. SCRAPING level 2

    def parse(self, response):
        apartments = json.loads(response.text)
        for apartment in apartments:
            url = apartment['permalink']
            title = apartment['name']
            property_type = 'apartment' if 'apartment' in apartment['property_type'] else 'house'
            landlord_email = apartment['client']['email']
            landlord_name = apartment['client']['name']
            landlord_phone = apartment['client']['phone']

            city = apartment['address']['city']
            province = apartment['address']['province']
            province_code = apartment['address']['province_code']
            zipcode = apartment['address']['postal_code']

            address = apartment['address']['address'] + \
                ', '+city+', '+province+', '+province_code
            description = description_cleaner(apartment['details']['overview'])

            latitude = apartment['geocode']['latitude']
            longitude = apartment['geocode']['longitude']

            available_date = apartment['availability_status_label']
            external_id = apartment['id']

            # *******************Scrape each page*********************
            request = Selector(requests.get(url).text)
            Amenty = " ".join(request.css(".amenity::text").getall())
            images = request.css(".gallery-image a::attr(href)").getall()
            i=1
            for suite in request.css('.table'):

                if not 'now' in suite.css('.modal-label::text').get().lower():
                    continue
                # *** Room_count ***
                rex = re.search(
                    r'\d+', suite.css('.suite-type .value::text').get())
                if rex:
                    room_count = rex[0]
                else:
                    room_count = '1'

                # *** rent ***
                rex = re.search(
                    r'\d+', suite.css('.suite-rate .value::text').get())
                if rex:
                    rent = rex[0]
                else:
                    rent = '1'

                available_date = suite.css(
                    '.suite-availability .available-date::text').get()

                external_link = url+'#'+str(i)
                i+=1

                if int(rent) > 0 and int(rent) < 25000:
                    item_loader = ListingLoader(response=response)

                    # # MetaData
                    item_loader.add_value(
                        "external_link", external_link)  # String
                    item_loader.add_value(
                        "external_source", self.external_source)  # String

                    item_loader.add_value(
                        "external_id", str(external_id))  # String
                    item_loader.add_value("position", self.position)  # Int
                    item_loader.add_value("title", title)  # String
                    item_loader.add_value("description", description)  # String

                    # # Property Details
                    item_loader.add_value("city", city)  # String
                    item_loader.add_value("zipcode", zipcode)  # String
                    item_loader.add_value("address", address)  # String
                    item_loader.add_value("latitude", str(latitude))  # String
                    item_loader.add_value(
                        "longitude", str(longitude))  # String
                    # item_loader.add_value("floor", floor)  # String
                    item_loader.add_value(
                        "property_type", property_type)  # String
                    # item_loader.add_value("square_meters", square_meters)  # Int
                    item_loader.add_value(
                        "room_count", int(float(room_count)))  # Int
                    # item_loader.add_value("bathroom_count", bathroom_count)  # Int

                    item_loader.add_value("available_date", available_date)

                    get_amenities(description, Amenty, item_loader)

                    # # Images
                    item_loader.add_value("images", images)  # Array
                    item_loader.add_value(
                        "external_images_count", len(images))  # Int
                    # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

                    # # Monetary Status
                    item_loader.add_value("rent", rent)  # Int
                    # item_loader.add_value("deposit", deposit)  # Int
                    # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                    # item_loader.add_value("utilities", utilities)  # Int
                    item_loader.add_value("currency", "EUR")  # String

                    # item_loader.add_value("water_cost", water_cost) # Int
                    # item_loader.add_value("heating_cost", heating_cost) # Int

                    # item_loader.add_value("energy_label", energy_label)  # String

                    # # LandLord Details
                    item_loader.add_value(
                        "landlord_name", landlord_name)  # String
                    item_loader.add_value(
                        "landlord_phone", landlord_phone)  # String
                    item_loader.add_value(
                        "landlord_email", landlord_email)  # String

                    self.position += 1
                    yield item_loader.load_item()
