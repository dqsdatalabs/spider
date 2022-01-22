# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address, extract_location_from_coordinates


class BriarlaneRentalSpider(scrapy.Spider):
    name = "Briarlane_Rental"
    start_urls = ['https://www.briarlane.ca']
    allowed_domains = []
    country = 'canada'
    locale = 'en'
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
        urls = response.css("a.btn.btn-info::attr(href)").getall()
        for url in urls:
            if '/Toronto/apartment' in url:
                url = self.start_urls[0] + url
                yield scrapy.Request(url, callback=self.populate_item)
    # 3. SCRAPING level 3
    def populate_item(self, response):
        address = response.css("div.row-fluid p::text").getall()[1]
        if address != None:
            long, lat = extract_location_from_address(address)
            longitude = str(long)
            latitude = str(lat)
            zipcode, city, no_address = extract_location_from_coordinates(longitude, latitude)

        description = response.css("div.span8::text").getall()[1].strip().split(
            'Call')[0]
        
        images = response.css('a.thumbnail img::attr(src)').getall()
        amenities = response.css('div.row-fluid ul li::text').getall()
        landlord_number = response.css(
            'div.row-fluid > p::text').getall()[-1].strip()
        landlord_name = 'Briarlane Rental Property Management Inc.'
        title = response.css("div h2::text").get()
        apartments_data = response.css(
            "table.table.table-striped tr")[1:]
        elevator = None
        washing_machine = None
        dishwasher = None
        pets_allowed = None
        balcony = None
        property_type = 'apartment'
        for amenity in amenities:
            if amenity.lower().find('elevator') != -1:
                elevator = True
            if amenity.lower().find('laundry') != -1:
                washing_machine = True
            if amenity.lower().find('pet') != -1:
                pets_allowed = True
            if amenity.lower().find('dishwasher') != -1:
                dishwasher = True
            if amenity.lower().find('balcony') != -1:
                balcony = True
        inx = 0
        images_floor_plan = [[]] * len(apartments_data)
        images_units = [[]] * len(apartments_data)
        index = 0
        images = response.css("div.item img::attr(src)").getall()
        for idx, img in enumerate(images):
            if 'floorplans' in img:
                images_floor_plan[index].append(img)
                if idx + 1 < len(images)and 'unitImages' in images[idx + 1]:
                    index += 1
            else:
                images_units[index].append(img)

        for apartment in apartments_data:
            beds = apartment.css('td::text').getall()[0].strip()
            room_count = 1
            if 'Two' in beds:
                room_count = 2
            if 'Three' in beds:
                room_count = 3
            if 'Four' in beds:
                room_count = 4            
            rent = apartment.css('td::text').getall()[1].strip()[1:].replace(',', '')
            if 'Waiting List' in rent:
                return
            rent = int(rent)
            parking = True
            parking_deposit = apartment.css('td::text').getall()[3].strip()[1:]
            if parking_deposit and 'door' in parking_deposit:
                parking_deposit = parking_deposit[parking_deposit.find("$") + 1: parking_deposit.find(",")]
            if parking_deposit and 'nquire' not in parking_deposit and 'applicable' not in parking_deposit:
                rent += int(parking_deposit.split(" ")[0])

            floor_plan_images = images_floor_plan[inx]
            images = images_units[inx]
            if len(floor_plan_images) == 0:
                floor_plan_images = None

            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_link", response.url + '#' + str(inx + 1) ) # String
            item_loader.add_value("external_source", self.external_source) # String
            item_loader.add_value("position", self.position) # Int
            item_loader.add_value("title", title) # String
            item_loader.add_value("description", description) # String
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", latitude) # String
            item_loader.add_value("longitude", longitude) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array
            item_loader.add_value("rent", rent) # Int
            # item_loader.add_value("deposit", deposit) # Int
            item_loader.add_value("currency", "CAD") # String
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_number) # String
            inx += 1

            self.position += 1
            yield item_loader.load_item()
