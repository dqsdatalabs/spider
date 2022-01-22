# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address


class HouseandroomPyspiderGermanySpider(scrapy.Spider):
    name = "HouseandRoom"
    start_urls = ['https://www.house-and-room.de/immobilien.xhtml']
    allowed_domains = ["house-and-room.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
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
        pages_number = response.css('div.jumpbox-frame p span a::text').extract()
        pages_number = pages_number[-1]
        pages_number = int(pages_number)
        urls = ['https://www.house-and-room.de/immobilien.xhtml']
        for i in range(pages_number-1):
            i = i+2
            page_url = 'https://www.house-and-room.de/immobilien.xhtml?p[obj0]=' + str(i)
            urls.append(page_url)
        for url in urls:
            yield scrapy.Request(url, callback=self.parse_pages, dont_filter=True)

    def parse_pages(self, response):
        apartments_divs = response.css('div.object-frame div.column.fourth')
        for apartment_div in apartments_divs:
            marketing_type = apartment_div.css('div.list-object input.object_marketing_type::attr(value)').extract()
            if "Rent" in marketing_type:
                apartment_url = apartment_div.css('div.list-object div.image a::attr(href)').extract()
                url = 'https://www.house-and-room.de/' + apartment_url[0]
                external_id = apartment_div.css('div.list-object::attr(id)').extract()
                city = apartment_div.css('div.list-object span::text').extract()
                square_room = apartment_div.css('div.list-object div.details.area-details')
                square_meters = square_room.xpath('.//div[1]//span[contains(@class, "object-area-wrapper")]//span[2]/text()').extract()
                room_count = square_room.xpath('.//div[2]//span//span/text()').extract()
                yield scrapy.Request(url, callback=self.populate_item, meta={
                    'external_id': external_id,
                    'city': city,
                    'square_meters': square_meters,
                    'room_count': room_count,
                })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        external_id = response.meta.get('external_id')
        external_id = external_id[0]
        external_id = external_id.split("-")[1]

        square_meters = response.meta.get('square_meters')
        square_meters = square_meters[0]
        square_meters = square_meters.split()[0]
        square_meters = square_meters.replace(".", "")
        square_meters = square_meters.replace(",", ".")
        square_meters = round(float(square_meters))
        square_meters = int(square_meters)

        room_count = response.meta.get('room_count')
        room_count = room_count[0]
        room_count = round(float(room_count))
        room_count = int(room_count)

        title = response.css('div.detail h2::text')[0].extract()
        balcony = None
        terrace = None
        if "balcony" in title.lower():
            balcony = True
        if "terrace" in title.lower():
            terrace = True

        images = response.css('div.gallery ul li a::attr(href)').extract()

        apartment_table_keys = response.css('div.details div.details-desktop table tr td strong::text').extract()
        apartment_table_values = response.css('div.details div.details-desktop table tr td span::text').extract()
        apartment_table = dict(zip(apartment_table_keys, apartment_table_values))

        furnished = None
        parking = None
        washing_machine = None
        dishwasher = None
        floor = None
        bathroom_count = None
        pets_allowed = None
        deposit = None
        heating_cost = None

        property_type = apartment_table['Property class']
        if "Apartments" in property_type:
            property_type = "apartment"
        elif "House" in property_type:
            property_type = "house"
        elif "Room" in property_type:
            property_type = "room"
        else:
            property_type = None

        zipcode = apartment_table['ZIP code']
        city = apartment_table['Town']

        if 'Floor' in apartment_table.keys():
            floor = apartment_table['Floor']

        if "Type of parking spaces" in apartment_table.keys():
            parking = True

        if 'Furnished' in apartment_table.keys():
            furnished_exist = apartment_table['Furnished']
            if "Full" in furnished_exist:
                furnished = True

        rent = apartment_table['Inclusive rent']
        rent = rent.split()[0]
        rent = rent.replace(".", "")
        rent = rent.replace(",", ".")
        rent = round(float(rent))
        rent = int(rent)

        if 'Rent including heating' in apartment_table.keys():
            warm_rent = apartment_table['Rent including heating']
            warm_rent = warm_rent.split()[0]
            warm_rent = warm_rent.replace(".", "")
            warm_rent = warm_rent.replace(",", ".")
            warm_rent = round(float(warm_rent))
            warm_rent = int(warm_rent)

            heating_cost = warm_rent - rent
            if heating_cost == 0:
                heating_cost = None

        available_date = apartment_table['Available from (date)']
        if "." in available_date:
            available_date = available_date.split(".")
            day = available_date[0]
            month = available_date[1]
            year = available_date[2]
            available_date = year + "-" + month + "-" + day
        else:
            available_date = "2022-01-04"

        if 'Pets' in apartment_table.keys():
            pets_allowed_str = apartment_table['Pets']
            if "Yes" in pets_allowed_str:
                pets_allowed = True

        if 'Number of bathrooms' in apartment_table.keys():
            bathroom_count = apartment_table['Number of bathrooms']
            bathroom_count = round(float(bathroom_count))
            bathroom_count = int(bathroom_count)

        if 'Equipment/Features' in apartment_table.keys():
            amenities = apartment_table['Equipment/Features']
            if "washing machine" in amenities:
                washing_machine = True
            if "dishwasher" in amenities:
                dishwasher = True

        if "Number of balconies/terraces" in apartment_table.keys():
            terrace = True
            balcony = True

        description_list = response.css('div.detail div.information span span::text').extract()
        description = []
        for item in description_list:
            if "You find many other" not in item:
                if "www.house and room.de" not in item:
                    if "www.house-and-room.de" not in item:
                        if "0581-9488593" not in item:
                            item = item.lower()
                            description.append(item)
        for item in description:
            if "deposit" in item:
                deposit_index = description.index(item)
                deposit_list = description[deposit_index:]
                deposit_list = " ".join(deposit_list)
                deposit_b = deposit_list.split("deposit")[1]
                deposit_b = deposit_b.split("\n")[0]
                if "month rent" in deposit_b:
                    deposit = deposit_b.split('month rent')[0]
                    deposit = deposit.split()[1]
                    deposit = deposit.strip()
                    if "one" in deposit:
                        deposit = None
                    else:
                        deposit = int(deposit)
                        deposit = deposit * rent
                elif "monthly rent" in deposit_b:
                    deposit = deposit_b.split('monthly rent')[0]
                    deposit = deposit.split()[1]
                    deposit = deposit.strip()
                    if "one" in deposit:
                        deposit = None
                    else:
                        deposit = int(deposit)
                        deposit = deposit * rent
                elif "months rent" in deposit_b:
                    deposit = deposit_b.split('months rent')[0]
                    deposit = deposit.split()[1]
                    deposit = deposit.strip()
                    deposit = int(deposit)
                    deposit = deposit * rent
                elif "eur" in deposit_b:
                    if deposit_b.startswith(": eur"):
                        deposit = deposit_b.split('eur')[1]
                        if deposit[-1] == ".":
                            deposit = deposit[:-1]
                        deposit = deposit.strip()
                        deposit = deposit.replace("-", "")
                        if ".000" in deposit:
                            deposit = deposit.replace(".", "")
                            deposit = deposit.replace(",", ".")
                            deposit = round(float(deposit))
                            deposit = int(deposit)
                        else:
                            deposit = deposit.replace(",", ".")
                            deposit = round(float(deposit))
                            deposit = int(deposit)
                    else:
                        deposit = deposit_b.split('eur')[0]
                        deposit = deposit.split()[1]
                        deposit = deposit.strip()
                        deposit = deposit.replace("-", "")
                        if ".000" in deposit:
                            deposit = deposit.replace(".", "")
                            deposit = deposit.replace(",", ".")
                            deposit = round(float(deposit))
                            deposit = int(deposit)
                        else:
                            deposit = deposit.replace(",", ".")
                            deposit = round(float(deposit))
                            deposit = int(deposit)
                elif "€" in deposit_b:
                    if deposit_b.startswith(": €"):
                        deposit = deposit_b.split('€')[1]
                        if deposit[-1] == ",":
                            deposit = deposit[:-1]
                        deposit = deposit.strip()
                        deposit = deposit.replace("-", "")
                        if ".000" in deposit:
                            deposit = deposit.replace(".", "")
                            deposit = deposit.replace(",", ".")
                            deposit = round(float(deposit))
                            deposit = int(deposit)
                        else:
                            deposit = deposit.replace(",", ".")
                            if deposit[-1] == ".":
                                deposit = deposit[:-1]
                            deposit = round(float(deposit))
                            deposit = int(deposit)
                    else:
                        deposit = deposit_b.split('€')[0]
                        deposit = deposit.split()[1]
                        deposit = deposit.strip()
                        deposit = deposit.replace("-", "")
                        if ".000" in deposit:
                            deposit = deposit.replace(".", "")
                            deposit = deposit.replace(",", ".")
                            deposit = round(float(deposit))
                            deposit = int(deposit)
                        else:
                            deposit = deposit.replace(",", ".")
                            deposit = round(float(deposit))
                            deposit = int(deposit)

        landlord_name = "House & Room Privatzimmervermittlung"
        landlord_number = "0581-9488593"
        landlord_email = "info@house-and-room.de"

        address = city + ", Germany"
        longitude, latitude = extract_location_from_address(address)
        longitude = str(longitude)
        latitude = str(latitude)

        # Enforces rent between 0 and 40,000 please dont delete these lines
        if int(rent) <= 0 and int(rent) > 40000:
            return

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
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
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
