# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import json
import scrapy
from urllib.parse import urlparse, urlunparse, parse_qs
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re


class Centre100Spider(scrapy.Spider):

    name = "centre100"
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        url = f'https://api.theliftsystem.com/v2/search?locale=en&client_id=623&auth_token=sswpREkUtyeYjeoahA2i&city_id=845&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=1500&min_sqft=0&max_sqft=100000&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments,+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=1118,845&pet_friendly=&offset=0&count=false'
        yield Request(url, callback=self.parse)

    # 2. SCRAPING level 2

    def parse(self, response):
        apartments = json.loads(response.text)
        
        for apartment in apartments:
            title = apartment['name']
            external_id = apartment['id']
            external_link = apartment['permalink']
            property_type = 'apartment' if 'apartment' in apartment['property_type'] else 'house'
            
            landlord_name = apartment['client']['name']
            landlord_email = apartment['client']['email']
            landlord_phone = apartment['client']['phone']

            city = apartment['address']['city']
            province =apartment['address']['province']
            zipcode =apartment['address']['postal_code']
            latitude=apartment['geocode']['latitude']
            longitude=apartment['geocode']['longitude']
            address = apartment['address']['address']+', '+city+', '+province+', '+zipcode

            available_date = apartment['availability_status_label']

            description = description_cleaner(apartment['details']['overview'])

            #-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*--*-*-*-*-*-*-*-*-*-
            request = Selector(requests.get(external_link).text)
            images = request.css(".gallery-image .cover::attr(style)").getall()
            images = [re.search(r'(http.+)\'',x).groups()[0] for x in images]
            i=1
            for suite in request.css(".suite-row "):
                try:
                    available_date = suite.css(".info-block:contains(vailability) .info a::text").get().replace(' ','').replace('\t','').replace('\n','')

                except:
                    pass


                regx = re.search(r'\d+',suite.css(".info-block:contains(ent) .info::text").get().replace('.',''))
                rent='0'
                if regx:
                    rent = regx[0]

                regx = re.search(r'\d+',suite.css(".info-block:contains(eet) .info::text").get())
                square_meters='0'
                if regx:
                    square_meters = regx[0]

                regx = re.search(r'\d+',suite.css(".info-block:contains(athroom) .info::text").get())
                bathroom_count='1'
                if regx:
                    bathroom_count = regx[0]
                else:
                    bathroom_count='1'

                regx = re.search(r'\d+',suite.css(".info-block:contains(edroom) .info::text").get())
                room_count='1'
                if regx:
                    room_count = regx[0]
                else:
                    room_count='1'

                if room_count=='0':
                    room_count='1'

                for img in suite.css(".info-block .suite-photo::attr(href)").getall():
                    images.append(img)
                floor_plan_images = suite.css(".info-block .floorplan-link::attr(href)").getall()
                floor_plan_images = [x for x in floor_plan_images if 'png' not in x]


                if int(rent) > 0 and int(rent) < 20000:
                    item_loader = ListingLoader(response=response)

                    # # MetaData
                    item_loader.add_value("external_link", external_link+'#'+str(i))  # String
                    item_loader.add_value(
                        "external_source", self.external_source)  # String

                    item_loader.add_value("external_id", str(external_id))  # String
                    item_loader.add_value("position", self.position)  # Int
                    item_loader.add_value("title", title)  # String
                    item_loader.add_value("description", description)  # String

                    # # Property Details
                    item_loader.add_value("city", city)  # String
                    item_loader.add_value("zipcode", zipcode)  # String
                    item_loader.add_value("address", address)  # String
                    item_loader.add_value("latitude", str(latitude))  # String
                    item_loader.add_value("longitude", str(longitude))  # String
                    #item_loader.add_value("floor", floor)  # String
                    item_loader.add_value("property_type", property_type)  # String
                    item_loader.add_value("square_meters", square_meters)  # Int
                    item_loader.add_value("room_count", room_count)  # Int
                    item_loader.add_value("bathroom_count", bathroom_count)  # Int

                    item_loader.add_value("available_date", available_date)

                    get_amenities(description, " ".join(request.css(".amenity-holder::text").getall()), item_loader)

                    # # Images
                    item_loader.add_value("images", images)  # Array
                    item_loader.add_value(
                        "external_images_count", len(images))  # Int
                    item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

                    # # Monetary Status
                    item_loader.add_value("rent", rent)  # Int
                    #item_loader.add_value("deposit", deposit)  # Int
                    # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                    #item_loader.add_value("utilities", utilities)  # Int
                    item_loader.add_value("currency", "CAD")  # String

                    # item_loader.add_value("water_cost", water_cost) # Int
                    # item_loader.add_value("heating_cost", heating_cost) # Int
                    # item_loader.add_value("energy_label", energy_label)  # String

                    # # LandLord Details
                    item_loader.add_value(
                        "landlord_name", landlord_name)  # String
                    item_loader.add_value(
                        "landlord_phone", landlord_phone)  # String
                    item_loader.add_value("landlord_email", landlord_email)  # String
                    
                    i+=1
                    self.position += 1
                    yield item_loader.load_item()
