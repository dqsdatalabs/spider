# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address


class ClaridgehomesPyspiderCanadaSpider(scrapy.Spider):
    name = "ClaridgeHomes"
    start_urls = ['https://rent.claridgehomes.com/searchlisting.aspx']
    allowed_domains = ["claridgehomes.com"]
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        apartments_divs = response.css('div#resultBody div.property-details div.property-detail')
        for apartment_div in apartments_divs:
            title = apartment_div.css('div div.search-box-details div.box-text div.prop-details div.prop-heading a.propertyName::text')[0].extract()
            url = apartment_div.css('div.prop-heading a.propertyName::attr(href)')[0].extract()
            if 'http' not in url:
                url = 'https://rent.claridgehomes.com/' + url
            address = apartment_div.css('div div.search-box-details div.box-text div.prop-address span.propertyAddress::text')[0].extract()
            city = apartment_div.css('div div.search-box-details div.box-text div.prop-address span.propertyCity::text')[0].extract()
            zipcode = apartment_div.css('div div.search-box-details div.box-text div.prop-address span.propertyZipCode::text')[0].extract()
            landlord_number = apartment_div.css('div div.search-box-details div.box-text div.prop-address span.prop-call-us::text')[0].extract()
            yield scrapy.Request(url, callback=self.populate_item, meta={
                'title': title,
                'address': address,
                'city': city,
                'zipcode': zipcode,
                'landlord_number': landlord_number,
            })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        building_name = response.meta.get('title')
        landlord_number = response.meta.get('landlord_number')

        address = response.meta.get('address')
        city = response.meta.get('city')
        zipcode = response.meta.get('zipcode')
        address = address + ', ' + zipcode + ' ' + city

        description = response.css('div#overview-content-container p::text').extract()
        if len(description) < 1:
            description = response.css('div#overview-content-container p font.normaltext::text').extract()

        suites_all = response.css('div#floorPlans-tab div div.accordion-inner div.tab-content div div div.tab-content div.tab-pane')
        for suites in suites_all:
            suites_name = suites.css('div h2::text')[0].extract()
            suites_count = suites.css('div div.availability-count::text')[0].extract()
            suites_table = suites.css('div table tr td::text').extract()
            if 'available' in suites_count.lower():
                item_loader = ListingLoader(response=response)
                property_type = 'apartment'
                room_count = suites_table[0]
                if 'Studio' in room_count:
                    room_count = 1
                    property_type = 'studio'
                else:
                    room_count = room_count.replace(",", ".")
                    room_count = round(float(room_count))
                    room_count = int(room_count)

                bathroom_count = suites_table[1]
                bathroom_count = bathroom_count.replace(",", ".")
                bathroom_count = round(float(bathroom_count))
                bathroom_count = int(bathroom_count)
                if bathroom_count == 0:
                    bathroom_count = None

                if '-' in suites_table[2]:
                    square_meters_list = []
                    square_meters = suites_table[2:4]
                    for item in square_meters:
                        item = item.strip()
                        item = item.replace("-", "")
                        item = item.replace(",", "")
                        item = round(float(item))
                        item = int(item)
                        square_meters_list.append(item)
                    square_meters = sum(square_meters_list) / len(square_meters_list)
                    square_meters = int(round(square_meters))
                    rent = suites_table[4:]
                else:
                    rent = suites_table[3:]
                    square_meters = suites_table[2]
                    if len(square_meters) > 1:
                        square_meters = square_meters.strip()
                        square_meters = square_meters.replace(",", "")
                        square_meters = round(float(square_meters))
                        square_meters = int(square_meters)
                    else:
                        square_meters = None

                deposit = None
                rent = ' '.join(rent)
                if '-' in rent:
                    rents = []
                    rent = rent.split('-')
                    for item in rent:
                        item = item.strip()
                        item = item.replace('$', '')
                        item = item.replace(",", "")
                        item = round(float(item))
                        item = int(item)
                        rents.append(item)
                    rent = sum(rents) / len(rents)
                    rent = int(round(rent))
                else:
                    rent = rent.replace('$', '')
                    if ' ' in rent:
                        rent_all = rent.split()
                        rent = rent_all[0]
                        rent = rent.replace(",", "")
                        rent = round(float(rent))
                        rent = int(rent)

                        deposit = rent_all[1]
                        deposit = deposit.replace(",", "")
                        deposit = round(float(deposit))
                        deposit = int(deposit)
                    else:
                        rent = rent.replace(",", "")
                        rent = round(float(rent))
                        rent = int(rent)
                # Enforces rent between 0 and 40,000 please dont delete these lines
                if int(rent) <= 0 and int(rent) > 40000:
                    return

                suites_name = suites_name.strip()
                suites_name = suites_name.replace('\n', '')
                if not bathroom_count:
                    if 'bath' in suites_name.lower():
                        bathroom_count = 1
                title = building_name + ', ' + suites_name

                external_link = response.url + '#' + suites_name

                floor_plan_images = suites.css('img.fp_thumb::attr(data-src)').extract()
                images_one = response.css('div#banner-image img::attr(data-src)').extract()

                amenities = response.css('div#amenities-tab div div div div ul li::text').extract()
                parking = None
                elevator = None
                swimming_pool = None
                washing_machine = None
                balcony = None
                dishwasher = None
                for amenity in amenities:
                    if 'parking' in amenity.lower():
                        parking = True
                    if 'elevator' in amenity.lower():
                        elevator = True
                    if 'pool' in amenity.lower():
                        swimming_pool = True
                    if 'washer' in amenity.lower():
                        washing_machine = True
                    if 'balcon' in amenity.lower():
                        balcony = True
                    if 'dishwasher' in amenity.lower():
                        dishwasher = True

                images_two = response.css('div#photos-tab img::attr(src)').extract()
                images = images_one + images_two

                landlord_name = 'Claridge Homes'

                longitude, latitude = extract_location_from_address(address)
                longitude = str(longitude)
                latitude = str(latitude)

                # # MetaData
                item_loader.add_value("external_link", external_link) # String
                item_loader.add_value("external_source", self.external_source) # String

                #item_loader.add_value("external_id", external_id) # String
                item_loader.add_value("position", self.position) # Int
                item_loader.add_value("title", title) # String
                item_loader.add_value("description", description) # String

                # # Property Details
                item_loader.add_value("city", city) # String
                item_loader.add_value("zipcode", zipcode) # String
                item_loader.add_value("address", address) # String
                item_loader.add_value("latitude", latitude) # String
                item_loader.add_value("longitude", longitude) # String
                #item_loader.add_value("floor", floor) # String
                item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
                item_loader.add_value("square_meters", square_meters) # Int
                item_loader.add_value("room_count", room_count) # Int
                item_loader.add_value("bathroom_count", bathroom_count) # Int

                #item_loader.add_value("available_date", available_date) # String => date_format

                #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                #item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking) # Boolean
                item_loader.add_value("elevator", elevator) # Boolean
                item_loader.add_value("balcony", balcony) # Boolean
                #item_loader.add_value("terrace", terrace) # Boolean
                item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                item_loader.add_value("washing_machine", washing_machine) # Boolean
                item_loader.add_value("dishwasher", dishwasher) # Boolean

                # # Images
                item_loader.add_value("images", images) # Array
                item_loader.add_value("external_images_count", len(images)) # Int
                item_loader.add_value("floor_plan_images", floor_plan_images) # Array

                # # Monetary Status
                item_loader.add_value("rent", rent) # Int
                item_loader.add_value("deposit", deposit) # Int
                #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                #item_loader.add_value("utilities", utilities) # Int
                item_loader.add_value("currency", "CAD") # String

                #item_loader.add_value("water_cost", water_cost) # Int
                #item_loader.add_value("heating_cost", heating_cost) # Int

                #item_loader.add_value("energy_label", energy_label) # String

                # # LandLord Details
                item_loader.add_value("landlord_name", landlord_name) # String
                item_loader.add_value("landlord_phone", landlord_number) # String
                #item_loader.add_value("landlord_email", landlord_email) # String

                self.position += 1
                yield item_loader.load_item()
