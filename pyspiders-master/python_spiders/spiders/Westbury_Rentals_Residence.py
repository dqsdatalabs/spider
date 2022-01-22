# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_unicode_char, extract_number_only



class WestburyRentalsResidenceSpider(scrapy.Spider):
    name = "Westbury_Rentals_Residence"
    start_urls = ['https://www.westburyrentals.com/searchlisting']
    allowed_domains = ["westburyrentals.com"]
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
        urls = response.css("div.span9 div.span10.prop-name a.AddClickTrackParams::attr(href)").getall()
        for url in urls:
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        title = response.xpath('//h1[@id="prop-name"]/text()').get()
        address = response.css("p.text-muted::text").get()
        address = remove_unicode_char(address)
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, _ = extract_location_from_coordinates(longitude, latitude)
        longitude = str(longitude)
        latitude = str(latitude)
        landlord_phone = response.css("div.fc-phonenumber a::text").get()
        landlord_name = 'Westbury Rentals Residence'
        landlord_email = 'Rentcafe@Firmcapital.Com'
        images = response.css("div.carousel-inner img::attr(data-src)").getall()
        images.append(response.css("div.carousel-inner img::attr(src)").get())
        amenities = response.css("div.amenities-section li::text").getall()
        description = ''
        for des in  response.css("div.overview-section.mb-50 p::text").getall():
            if des.strip() != '':
                description += des

        floor_plans = response.css("div.text-center.mb-20 img::attr(src)").getall()
        for inx, img in enumerate(floor_plans):
            floor_plans[inx] = 'https://cdngeneralcf.rentcafe.com' + img
        beds = response.css('p.text-muted').css('span:nth-child(1)').css("::text").getall()  
        for inx, bed in enumerate(beds):
            beds[inx] = bed[0]
        rents = response.css("div.span4 p.text-md::text").getall()
        for inx, rnt in enumerate(rents):
            rnt = rnt[1:]
            rents[inx] =  rnt.split(" ")[0]
        property_type = 'apartment'
        baths = response.css('p.text-muted').css('span:nth-child(3)').css("::text").getall()
        for inx, bath in enumerate(baths):
            baths[inx] = bath[0]

        pets_allowed = False
        furnished = None
        parking = False
        elevator = None
        balcony = None
        swimming_pool = None
        washing_machine = None
        dishwasher = None
        terrace = None
        for amenity in amenities:
            if 'Dog park' in amenity or 'pet' in amenity:
                pets_allowed = True
            if 'parking' in amenity:
                parking = True
            if 'furnished' in amenity:
                furnished = True
            if 'elevator' in amenity:
                elevator = True
            if 'balcony' in amenity:
                balcony = True
            if 'swimming_pool' in amenity:
                swimming_pool = True
            if 'Laundry' in amenity:
                washing_machine = True
            if 'dishwasher' in amenity:
                dishwasher = True
            if 'terrace' in amenity:
                terrace = True
        if 'Dog park' in description or 'pet' in description:
            pets_allowed = True
        if 'parking' in description:
            parking = True
        if 'furnished' in description:
            furnished = True
        if 'elevator' in description:
            elevator = True
        if 'balcony' in description:
            balcony = True
        if 'swimming_pool' in description:
            swimming_pool = True
        if 'Laundry' in description:
            washing_machine = True
        if 'dishwasher' in description:
            dishwasher = True
        if 'terrace' in description:
            terrace = True
        for index in range(len(rents)):
            item_loader = ListingLoader(response=response)

            room_count = beds[index]
            if room_count == 0 or room_count == '0' or room_count == None:
                room_count = 1
            rent = rents[index]
            if rent == None or rent == 0 or rent == '0':
                return
            
            bathroom_count = baths[index]
            floor_plan_images = floor_plans[index]
            # # MetaData
            item_loader.add_value("external_link", response.url+ '#' + str(index + 1)) # String
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
            #item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            #item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            item_loader.add_value("terrace", terrace) # Boolean
            item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            #item_loader.add_value("deposit", deposit) # Int
            #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            #item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD") # String

            #item_loader.add_value("water_cost", water_cost) # Int
            #item_loader.add_value("heating_cost", heating_cost) # Int

            #item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
