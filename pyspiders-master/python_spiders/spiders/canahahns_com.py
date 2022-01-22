# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *
import json
from scrapy.http import HtmlResponse


class CanahahnsComSpider(scrapy.Spider):
    name = "canahahns_com2"
    start_urls = ['https://api.theliftsystem.com/v2/search?locale=en&client_id=23&auth_token=sswpREkUtyeYjeoahA2i&limit=300']
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        props = json.loads(response.text)
        prop_types = ["apartment", "house", "room", "student_apartment", "studio"]

        for prop in props:
            item_loader = ListingLoader(response=response)

            # Check if available
            if prop['availability_status'] != 1:
                continue

            # External Id, title, external_link
            external_id = str(prop['id'])
            title = prop['name']
            external_link = prop['permalink']

            # property type
            property_type = 'apartment'
            prp_type = prop['property_type']

            t = 0
            k = ''
            for i in prop_types:
                if i in prp_type:
                    t = 1
                    k = i
            if t == 0:
                continue
            else:
                property_type = k

            # longitude, latitude, zipcode, city, address
            longitude, latitude = prop['geocode']['longitude'], prop['geocode']['latitude'],
            zipcode, city, address = extract_location_from_coordinates(float(longitude), float(latitude))

            # images
            images = [prop['photo_path']]

            # Description
            description = prop['details']['overview']

            # bathroom_count
            bathroom_count = len(prop['matched_baths'])

            # room_count, bathroom_count, rent, square_feet
            room_count = max(1, int(float(prop['statistics']['suites']['bedrooms']['average'])))
            rent = int(float(prop['statistics']['suites']['rates']['average']))
            square_feet = max(int(float(prop['statistics']['suites']['square_feet']['average'])),
                              int(float(prop['statistics']['suites']['square_feet']['min'])))

            # pets_allowed
            pets_allowed = prop['pet_friendly']

            # parking
            parking = None
            if prop['parking']['indoor'] is not None or prop['parking']['outdoor'] is not None:
                parking = True



            # available_date
            available_date = None
            if len(prop['min_availability_date']) > 0:
                if len(prop['min_availability_date']) > 0:
                    available_date = prop['min_availability_date']

            # room_count, bathroom_count, rent, square_feet
            room_count = max(1, int(float(prop['statistics']['suites']['bedrooms']['average'])))
            bathroom_count = max(int(prop['statistics']['suites']['bathrooms']['max']), int(prop['statistics']['suites']['bathrooms']['average']))
            rent = int(prop['statistics']['suites']['rates']['average'])
            square_feet = max(int(prop['statistics']['suites']['square_feet']['average']),
                              int(float(prop['statistics']['suites']['square_feet']['min'])))

            rents = [int(prop['statistics']['suites']['rates']['min']) , int(prop['statistics']['suites']['rates']['max'])]
            sub_square_feet, sub_bath, sub_rooms, sub_rooms_names, floor_image, overview, full_images, sub_images = self.sub_props(external_link)

            images = images + full_images
            # balcony, terrace, washing_machine, dishwasher, parking, elevator
            balcony = terrace = washing_machine = dishwasher = elevator = swimming_pool = None
            pets_names = ['pet friendly', 'pets friendly', 'pets allowed', 'pet allowed', 'cats friendly',
                          'cat friendly', 'dogs fiendly', 'dog fiendly', 'cats allowed', 'cat allowed',
                          'dogs allowed',
                          'dog allowed']

            balcony_names = ['balcony', 'balconies']

            terraces = ['terrace', 'terraces']

            pool_names = ['pool', 'swimming', 'swimming pool']

            washing_names = ['washing machine', 'loundry', 'washer']

            dishwasher_names = ['dishwasher']

            park_names = ['parking', 'garage']

            elevator_names = ['elevator']

            if re.search(r"(?=(" + '|'.join(pets_names) + r"))", description) or re.search(r"(?=(" + '|'.join(pets_names) + r"))", overview):
                pets_allowed = True

            if re.search(r"(?=(" + '|'.join(balcony_names) + r"))", description) or re.search(r"(?=(" + '|'.join(balcony_names) + r"))", overview):
                balcony = True

            if re.search(r"(?=(" + '|'.join(terraces) + r"))", description) or re.search(r"(?=(" + '|'.join(terraces) + r"))", overview):
                terrace = True

            if re.search(r"(?=(" + '|'.join(washing_names) + r"))", description) or re.search(r"(?=(" + '|'.join(washing_names) + r"))", overview) :
                washing_machine = True

            if re.search(r"(?=(" + '|'.join(dishwasher_names) + r"))", description) or re.search(r"(?=(" + '|'.join(dishwasher_names) + r"))", overview):
                dishwasher = True

            if re.search(r"(?=(" + '|'.join(park_names) + r"))", description) or re.search(r"(?=(" + '|'.join(park_names) + r"))", overview) :
                parking = True

            if re.search(r"(?=(" + '|'.join(elevator_names) + r"))", description) or re.search(r"(?=(" + '|'.join(elevator_names) + r"))", overview):
                elevator = True

            if re.search(r"(?=(" + '|'.join(pool_names) + r"))", description) or re.search(r"(?=(" + '|'.join(pool_names) + r"))", description):
                swimming_pool = True





            for i in range(len(floor_image)):
                item_loader = ListingLoader(response=response)
                final_images = images
                if rents[i]:
                    rent = int(float(rents[i]))
                title = title + ' ' + sub_rooms_names[i]
                if sub_square_feet[i]:
                    square_feet = int(float(sub_square_feet[i]))
                if sub_bath[i]:
                    bathroom_count = int(float(sub_bath[i]))
                if sub_rooms[i]:
                    room_count = int(float(sub_rooms[i]))
                floor_plan_images = None
                if floor_image[i]:
                    floor_plan_images = [floor_image[i]]
                if sub_images[i]:
                    final_images.append(sub_images[i])
                url = external_link+'#'+str(self.position)

                # Landlord info
                landlord_name = prop['client']['name']
                landlord_number = prop['client']['phone']
                landlord_email = prop['client']['email']

                # # MetaData
                item_loader.add_value("external_link", url) # String
                item_loader.add_value("external_source", self.external_source) # String

                item_loader.add_value("external_id", external_id) # String
                item_loader.add_value("position", self.position) # Int
                item_loader.add_value("title", title) # String
                item_loader.add_value("description", description) # String

                # # Property Details
                item_loader.add_value("city", city) # String
                item_loader.add_value("zipcode", zipcode) # String
                item_loader.add_value("address", address) # String
                item_loader.add_value("latitude", latitude) # String
                item_loader.add_value("longitude", longitude) # String
                # item_loader.add_value("floor", floor) # String
                item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
                item_loader.add_value("square_meters", square_feet) # Int
                item_loader.add_value("room_count", room_count) # Int
                item_loader.add_value("bathroom_count", bathroom_count) # Int

                item_loader.add_value("available_date", available_date) # String => date_format

                item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                # item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking) # Boolean
                item_loader.add_value("elevator", elevator) # Boolean
                item_loader.add_value("balcony", balcony) # Boolean
                item_loader.add_value("terrace", terrace) # Boolean
                item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                item_loader.add_value("washing_machine", washing_machine) # Boolean
                item_loader.add_value("dishwasher", dishwasher) # Boolean

                # # Images
                item_loader.add_value("images", list(set(final_images))) # Array
                item_loader.add_value("external_images_count", len(images)) # Int
                item_loader.add_value("floor_plan_images", floor_plan_images) # Array

                # # Monetary Status
                item_loader.add_value("rent", rent) # Int
                # item_loader.add_value("deposit", deposit) # Int
                # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                #item_loader.add_value("utilities", utilities) # Int
                item_loader.add_value("currency", "CAD") # String

                #item_loader.add_value("water_cost", water_cost) # Int
                #item_loader.add_value("heating_cost", heating_cost) # Int

                #item_loader.add_value("energy_label", energy_label) # String

                # # LandLord Details
                item_loader.add_value("landlord_name", landlord_name) # String
                item_loader.add_value("landlord_phone", landlord_number) # String
                item_loader.add_value("landlord_email", landlord_email) # String

                self.position += 1
                yield item_loader.load_item()


    def sub_props(self, url):
        resp = requests.get(url)

        response = HtmlResponse(url="", body=resp.text, encoding='utf-8')
        sub_square_feet= [re.search(r'\d+', i)[0] if re.search(r'\d+', i) else '1' for i in [i for i in [i.strip() for i in response.css('.info-block:nth-child(3)').extract() if i is not None] if i != '']]
        sub_bath = [re.search(r'\d+', i)[0] if re.search(r'\d+', i) else '1' for i in [i for i in [i.strip() for i in response.css('.info-block:nth-child(2)').extract() if i is not None] if i != '']]
        sub_rooms = [re.search(r'\d+', i)[0] if re.search(r'\d+', i) else '1' for i in [i for i in [i.strip() for i in response.css('.info-block:nth-child(1)').extract() if i is not None] if i != '']]

        sub_rooms_names = response.css('.suite-box .suite-type::text').extract()
        floor_image = [re.search(r'(http|ftp|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-])', i)[0] if re.search(r'(http|ftp|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-])', i) else None for i in  response.css('.info-block:nth-child(5)').extract()]
        over_view = ' '.join([i.strip() for i in response.css('.amenity-holder::text').extract()])
        full_images = [re.search(r'(http|ftp|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-])', i)[0] if re.search(r'(http|ftp|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-])', i) else None for i in response.css('#slickslider-default-id-0 .cover').extract()]
        sub_images = [re.search(r'(http|ftp|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-])', i)[0] if re.search(r'(http|ftp|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-])', i) else None for i in response.css('.info-block:nth-child(6)').extract()]

        return sub_square_feet, sub_bath, sub_rooms, sub_rooms_names, floor_image, over_view, full_images, sub_images