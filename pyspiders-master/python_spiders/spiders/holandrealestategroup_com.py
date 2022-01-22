# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *
import json
from scrapy.http import HtmlResponse



class AvenuelivingCaSpider(scrapy.Spider):
    name = "holandrealestategroup_com"
    start_urls = ['https://api.theliftsystem.com/v2/search?client_id=676&auth_token=sswpREkUtyeYjeoahA2i&limit=500']
    allowed_domains = ["holandrealestategroup.com"]

    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
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
            if prop['availability_status'] != 1:
                continue
            # property_type
            property_type = prop['property_type']
            t = 0
            k = ''
            for i in prop_types:
                if i in property_type:
                    t = 1
                    k = i
            if t == 0:
                continue
            else:
                property_type = k


            # External Id, title, external_link
            external_id = prop['id']
            title = prop['name']
            external_link = prop['permalink']

            # longitude, latitude, zipcode, city, address
            longitude, latitude = prop['geocode']['longitude'], prop['geocode']['latitude'],
            zipcode, city, address = extract_location_from_coordinates(float(longitude), float(latitude))

            # images
            images = [prop['photo_path']]

            # Description
            description = prop['website']['description']

            # room_count, bathroom_count, rent, square_feet
            room_count = max(1, int(float(prop['statistics']['suites']['bedrooms']['average'])))
            rent = int(float(prop['statistics']['suites']['rates']['average']))
            square_feet = max(int(float(prop['statistics']['suites']['square_feet']['average'])),
                              int(float(prop['statistics']['suites']['square_feet']['min'])))

            # Landlord info
            landlord_name = prop['client']['name']
            landlord_number = prop['client']['phone']
            landlord_email = prop['client']['email']

            # pets_allowed
            pets_allowed = None if prop['pet_friendly'] == 'n/a' else prop['pet_friendly']

            # parking
            parking = None
            if prop['parking']['indoor'] is not None or prop['parking']['outdoor'] is not None:
                parking = True

            # available_date
            available_date = None
            if len(prop['min_availability_date']) > 0:
                available_date = prop['min_availability_date']

            # bathroom_count
            bathroom_count = len(prop['matched_baths'])

            # overview
            overview = prop['details']['overview']

            description = overview+' '+description if description is not None else overview

            # sub property
            sub_rent, sub_square_meters, sub_rooms, sub_rooms_names, sub_image, amenity = self.sub_props(external_link)

            # balcony, terrace, washing_machine, dishwasher, parking, elevator
            balcony = terrace = washing_machine = dishwasher = elevator = swimming_pool = furnished = None
            pets_names = ['pet friendly', 'pets friendly', 'pets allowed', 'pet allowed', 'cats friendly',
                          'cat friendly', 'dogs friendly', 'dog friendly', 'cats allowed', 'cat allowed', 'dogs allowed',
                          'dog allowed']

            balcony_names = ['balcony', 'balconies']

            terraces = ['terrace', 'terraces']

            pool_names = ['pool', 'swimming', 'swimming pool']

            washing_names = ['washing machine', 'loundry', 'washer']

            dishwasher_names = ['dishwasher']

            park_names = ['parking', 'garage']

            elevator_names = ['elevator']

            if re.search(r"(?=(" + '|'.join(pets_names) + r"))", description.lower()+' '.join(amenity).lower()):
                pets_allowed = True

            if re.search(r"(?=(" + '|'.join(balcony_names) + r"))", description.lower()+' '.join(amenity).lower()):
                balcony = True

            if re.search(r"(?=(" + '|'.join(terraces) + r"))", description.lower()+' '.join(amenity).lower()):
                terrace = True

            if re.search(r"(?=(" + '|'.join(washing_names) + r"))", description.lower()+' '.join(amenity).lower()):
                washing_machine = True

            if re.search(r"(?=(" + '|'.join(dishwasher_names) + r"))", description.lower()+' '.join(amenity).lower()):
                dishwasher = True

            if re.search(r"(?=(" + '|'.join(park_names) + r"))", description.lower()+' '.join(amenity).lower()):
                parking = True

            if re.search(r"(?=(" + '|'.join(elevator_names) + r"))", description.lower()+' '.join(amenity).lower()):
                elevator = True

            if re.search(r"(?=(" + '|'.join(pool_names) + r"))", description.lower()+' '.join(amenity).lower()):
                swimming_pool = True
            if 'unfurnished' in description.lower():
                furnished = False
            if 'furnished' in description.lower():
                furnished = True


            for i in range(len(sub_rent)):
                link = external_link
                all_images = images
                item_loader = ListingLoader(response=response)
                if sub_rent[i]:
                    rent = int(float(sub_rent[i].replace('$', '')))
                    title = title +' '+sub_rooms_names[i]
                    if 'unfurnished' in sub_rooms_names[i].lower():
                        furnished = False
                    if 'furnished' in sub_rooms_names[i].lower():
                        furnished = True
                    if sub_square_meters[i]:
                        square_feet = int(float(sub_square_meters[i]))
                    if sub_rooms[i]:
                        room_count = int(float(sub_rooms[i]))
                    if sub_image[i]:
                        all_images = sub_image[i]+images
                    link = external_link + '#'+str(self.position)
                square_feet = None if square_feet==0 else square_feet
                if rent ==0 :
                    continue
                item_loader.add_value("external_link", link)  # String
                item_loader.add_value("external_source", self.external_source)  # String

                item_loader.add_value("external_id", str(external_id))  # String
                item_loader.add_value("position", self.position)  # Int
                item_loader.add_value("title", title)  # String
                item_loader.add_value("description", description)  # String

                # # Property Details
                item_loader.add_value("city", city)  # String
                item_loader.add_value("zipcode", zipcode)  # String
                item_loader.add_value("address", address)  # String
                item_loader.add_value("latitude", latitude)  # String
                item_loader.add_value("longitude", longitude)  # String
                # item_loader.add_value("floor", floor) # String
                item_loader.add_value("property_type",
                                      property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
                item_loader.add_value("square_meters", square_feet)  # Int
                item_loader.add_value("room_count", room_count)  # Int
                item_loader.add_value("bathroom_count", bathroom_count)  # Int

                item_loader.add_value("available_date", available_date)  # String => date_format

                item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
                item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking)  # Boolean
                item_loader.add_value("elevator", elevator)  # Boolean
                item_loader.add_value("balcony", balcony)  # Boolean
                item_loader.add_value("terrace", terrace)  # Boolean
                item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
                item_loader.add_value("washing_machine", washing_machine)  # Boolean
                item_loader.add_value("dishwasher", dishwasher)  # Boolean

                # # Images
                item_loader.add_value("images", all_images)  # Array
                item_loader.add_value("external_images_count", len(images))  # Int
                # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

                # # Monetary Status
                item_loader.add_value("rent", rent)  # Int
                # item_loader.add_value("deposit", deposit) # Int
                # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                # item_loader.add_value("utilities", utilities) # Int
                item_loader.add_value("currency", "CAD")  # String

                # item_loader.add_value("water_cost", water_cost) # Int
                # item_loader.add_value("heating_cost", heating_cost) # Int

                # item_loader.add_value("energy_label", energy_label) # String

                # # LandLord Details
                item_loader.add_value("landlord_name", landlord_name)  # String
                item_loader.add_value("landlord_phone", landlord_number)  # String
                item_loader.add_value("landlord_email", landlord_email)  # String

                self.position += 1
                yield item_loader.load_item()

    def sub_props(self, url):
        resp = requests.get(url)
        response = HtmlResponse(url="", body=resp.text, encoding='utf-8')
        sub_rent = [re.search(r'\$[0-9]+', i)[0] if re.search(r'\$[0-9]+', i) else None for i in response.css('.suite-rate').extract()]
        sub_square_meters = [i.split()[0] if len(i)>2 else None for i in [i.strip() for i in response.css('.sq-ft::text').extract() ]]
        sub_rooms = [re.search(r'\d+', i)[0] if re.search(r'\d+', i) else '1' for i in [i for i in [i.strip() for i in response.css('.type-name::text').extract() if i is not None] if i != '']]
        sub_rooms_names = [i for i in [i.strip() for i in response.css('.type-name::text').extract() if i is not None]]
        sub_images = []
        amenity = ' '.join(response.css('.amenity::text').extract())
        for i in range(1, len(sub_rent)+1):
            sub_images.append(response.xpath(f'//*[@id="content"]/section/div/section[1]/div[2]/div/div/div[1]/div[4]/div/a[{str(i)}]/@href').extract())
        return sub_rent, sub_square_meters, sub_rooms, sub_rooms_names, sub_images, amenity

