# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re
import json


class HazelviewpropertiesSpider(scrapy.Spider):

    name = "hazelviewproperties"

    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    allowed_domains=['hazelviewproperties.com']

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):

        url = f'https://lift-api.rentsync.com/v2/search/map_pins?locale=en&only_available_suites=&show_all_properties=&client_id=497&auth_token=sswpREkUtyeYjeoahA2i&city_ids=329,387,408,658,765,845,1154,1170,1174,1425,1607,1615,1837,1863,2042,2084,2213,2356,2377,2555,3133,3218,3284,3370,3377&geocode=&min_bed=-1&max_bed=5&min_bath=-1&max_bath=10&min_rate=0&max_rate=10000&property_types=low-rise-apartment,mid-rise-apartment,high-rise-apartment,multi-unit-house,luxury-apartment,single-family-home,duplex,triplex,townhouse,rooms,semi&region=&keyword=&order=min_rate+ASC,+max_rate+ASC,+min_bed+ASC,+max_bed+ASC&limit=9999&offset=0&count=false&show_custom_fields=false&show_amenities=false'
        yield Request(url,
            callback=self.parseApartment,
            dont_filter=True)

    # 2. SCRAPING level 2

    def parse(self, response):
        pass

    def parseApartment(self, response):
        
        apartments = json.loads(response.text)

        for apartment in apartments:

            external_id = apartment['id']
            external_link = apartment['permalink']
            title = apartment['website']['title']
            city = apartment['address']['city']
            postal_code = apartment['address']['postal_code']
            address = apartment['address']['address']+', '+city+', '+apartment['address']['province_code']+', '+postal_code
            latitude = apartment['geocode']['latitude']
            longitude = apartment['geocode']['longitude']

            datausage = {
                'external_id':external_id,
                'title': title,
                'latitude': latitude,
                'address': address,
                'longitude': longitude,
                'city': city,
                'postal_code':postal_code
            }

            yield Request(external_link, meta=datausage, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        print(response.url)

        external_id=str(response.meta['external_id'])
        title=response.meta['title']
        latitude=response.meta['latitude']
        address=response.meta['address']
        longitude=response.meta['longitude']
        city=response.meta['city']
        zipcode=response.meta['postal_code']

        i = 1
        images = response.css(".gallery-image .cover::attr(style)").getall()
        try:
            images = [x for x in images if 'floorplans' not in x]
            images = [x.replace('background-image:url(','').replace('\'','').replace(' ','').replace(')','') for x in images]
        except:
            pass
        


        description = remove_white_spaces("".join(response.css('.container-fluid.capped .building-description p::text').getall()))
        landlord_phone = response.css("a.phone::attr(href)")
        if landlord_phone:
            landlord_phone = landlord_phone.get().replace('tel:','')
        else:
            landlord_phone = '1-866-898-8868'

        if len(response.css(".unit-block"))==0:
            return
        for unit in response.css(".unit-block"):
            rent='0'
            try:
                rent = re.search(r'\d+',"".join(unit.css('.content .rate .value *::text').getall()))[0]
            except:
                rent='0'

            
            external_link = response.url+"#"+str(i)
            room_count = unit.css('.content .beds .value::text').get()
            if not room_count or room_count=='' or len(room_count)==0:
                room_count='1'
            bathroom_count = unit.css('.content .baths .value::text').get()
            square_meters = unit.css('.content .sqft .value::text').get()
            if square_meters:
                square_meters = re.search(r'\d+',square_meters)[0]
            
            floor_plan_images = unit.css(".image .cover::attr(style)").getall()
            try:
                floor_plan_images = [x.replace('background-image:url(','').replace('\'','').replace(' ','').replace(')','') for x in floor_plan_images]
            except:
                pass


            property_type = 'apartment'
            i+=1

            if int(rent) > 0 and int(rent) < 20000:
                item_loader = ListingLoader(response=response)

                # # MetaData
                item_loader.add_value("external_link", external_link)  # String
                item_loader.add_value(
                    "external_source", self.external_source)  # String

                item_loader.add_value("external_id", external_id)  # String
                item_loader.add_value("position", self.position)  # Int
                item_loader.add_value("title", title)  # String
                item_loader.add_value("description", description)  # String

                # # Property Details
                item_loader.add_value("city", city)  # String
                item_loader.add_value("zipcode", zipcode)  # String
                item_loader.add_value("address", address)  # String
                item_loader.add_value("latitude", latitude)  # String
                item_loader.add_value("longitude", longitude)  # String
                #item_loader.add_value("floor", floor)  # String
                item_loader.add_value("property_type", property_type)  # String
                item_loader.add_value("square_meters", square_meters)  # Int
                item_loader.add_value("room_count", room_count)  # Int
                item_loader.add_value("bathroom_count", bathroom_count)  # Int

                #item_loader.add_value("available_date", available_date)

                self.get_features_from_description(
                    description+" ".join(response.css(".lt_content.lt_dettagli strong::text").getall()), response, item_loader)

                # # Images
                item_loader.add_value("images", images)  # Array
                item_loader.add_value("external_images_count", len(images))  # Int
                item_loader.add_value("floor_plan_images", floor_plan_images) # Array

                # # Monetary Status
                item_loader.add_value("rent", rent)  # Int
                # item_loader.add_value("deposit", deposit) # Int
                # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                # item_loader.add_value("utilities", utilities)  # Int
                item_loader.add_value("currency", "CAD")  # String

                # item_loader.add_value("water_cost", water_cost) # Int
                # item_loader.add_value("heating_cost", heating_cost) # Int

                #item_loader.add_value("energy_label", energy_label)  # String

                # # LandLord Details
                item_loader.add_value(
                    "landlord_name", 'Hazelview properties')  # String
                item_loader.add_value(
                    "landlord_phone",landlord_phone)  # String
                item_loader.add_value(
                    "landlord_email", 'dcampbell@hazelview.com')  # String

                self.position += 1
                yield item_loader.load_item()

    Amenties = {
        'pets_allowed': ['pet'],
        'furnished': ['furnish', 'MÖBLIERTES'.lower()],
        'parking': ['parking', 'garage','parcheggio'],
        'elevator': ['elevator', 'aufzug'],
        'balcony': ['balcon', 'balkon'],
        'terrace': ['terrace','terrazz'],
        'swimming_pool': ['pool'],
        'washing_machine': [' washer', 'laundry', 'washing_machine', 'waschmaschine'],
        'dishwasher': ['dishwasher', 'geschirrspüler']
    }

    def get_features_from_description(self, description, response, item_loader):
        description = description.lower()
        pets_allowed = True if any(
            x in description for x in self.Amenties['pets_allowed']) else False
        furnished = True if any(
            x in description for x in self.Amenties['furnished']) else False
        parking = True if any(
            x in description for x in self.Amenties['parking']) else False
        elevator = True if any(
            x in description for x in self.Amenties['elevator']) else False
        balcony = True if any(
            x in description for x in self.Amenties['balcony']) else False
        terrace = True if any(
            x in description for x in self.Amenties['terrace']) else False
        swimming_pool = True if any(
            x in description for x in self.Amenties['swimming_pool']) else False
        washing_machine = True if any(
            x in description for x in self.Amenties['washing_machine']) else False
        dishwasher = True if any(
            x in description for x in self.Amenties['dishwasher']) else False

        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        item_loader.add_value("furnished", furnished)  # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean
        return pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher
