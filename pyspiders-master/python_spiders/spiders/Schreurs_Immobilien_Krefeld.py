# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import scrapy
from ..loaders import ListingLoader
from ..helper import description_cleaner, extract_number_only, extract_rent_currency, extract_location_from_address, extract_location_from_coordinates, get_amenities, get_price

class SchreursImmobilienKrefeldSpider(scrapy.Spider):
    name = "Schreurs_Immobilien_Krefeld"
    start_urls = ['https://www.schreurs-immobilien.de/mieten/wohnungen/']
    allowed_domains = ["schreurs-immobilien.de"]
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
        urls = response.css('div.listing-box a::attr(href)').getall()
        for url in urls:
            yield scrapy.Request('https://www.schreurs-immobilien.de' + url, callback=self.populate_item)


    # 3. SCRAPING level 3
    def populate_item(self, response):
        heating_cost = washing_machine = address = property_type = pets_allowed = balcony = terrace = elevator = external_id = floor = parking = None
        address = property_type = pets_allowed = balcony = terrace = elevator = external_id = floor = parking = energy_label = None
        utilities  = available_date = deposit = total_rent = rent = currency = square_meters = total_square_meters = landlord_email = landlord_name = landlord_phone = None
        room_count = 1
        bathroom_count = 1
        property_type = 'apartment'
        keys = response.css('div.overview li strong::text').getall()
        vals = response.css('div.overview li span::text').getall()
        amenties = ''
        for row in zip(keys, vals):
            key = row[0].strip().lower()
            val = row[1].strip().lower()
            if 'objektart' in key:
                if 'wohnung' in val:
                   property_type = 'apartment'
                else:
                    property_type = 'house'
            elif 'wohnfläche' in key:
                square_meters = int(float(extract_number_only(
                    val, thousand_separator=',', scale_separator='.')))
            elif 'grundstück' in key:
                total_square_meters = int(float(extract_number_only(
                    val, thousand_separator=',', scale_separator='.')))
            elif 'objekt-nr.' in key:
                external_id = val
            elif "badezimmer" in key:
                bathroom_count = int(float(val.split(',')[0]))
            elif "schlafzimmer" in key:
                room_count = int(float(val.split(',')[0]))
            elif 'ort' in key:
                address = val
            elif 'etagenzahl' in key:
                floor = val.strip()[0]
                floor = None if not floor.isnumeric() else floor
            elif 'balkone' in key:
                if int(val[0]) > 0:
                    amenties += 'balkone'
            elif 'stellplatzanzahl' in key:
                if int(val[0]) > 0:
                    amenties += ' Stellplatzanzahl'
            elif "kaltmiete" in key:
                if 'vermietet' in val:
                    return
                rent, currency = extract_rent_currency(
                    val, self.country, SchreursImmobilienKrefeldSpider)
                rent = get_price(val)
            elif "warmmiete" in key:
                total_rent, currency = extract_rent_currency(
                    val, self.country, SchreursImmobilienKrefeldSpider)
                total_rent = get_price(val)
            elif "nebenkosten" in key:
                utilities = get_price(val)
            elif 'heizkosten' in key:
                heating_cost = get_price(val)
        if rent is None:
            if total_rent is not None: 
                rent = total_rent
            else:
                return
        if square_meters is None:
            if total_square_meters is not None: 
                square_meters = total_square_meters
        status = response.css('div.listing-box-image-label::text').get()
        if status:
            if status.lower() == 'reserviert' or status.lower() == 'vermietet':
                return
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(
            longitude=longitude, latitude=latitude)
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        descriptions = response.css('div.listing-detail p::text').getall()
        description = ''
        for text in descriptions:
            description += text.strip() + ' '
        images = response.css('a.fancybox img::attr(src)').getall()
        for i in range(len(images)):
            images[i] = 'https://www.schreurs-immobilien.de' + images[i]
        title = response.css('h1.expose::text').get()
        amenties_energy = response.css('ul.amenities li::text').getall()
        for text in amenties_energy:
            amenties += ' ' + text
            if 'energiekennwert' in text.lower():
                energy_label = text.split(' ')[1]  
        description = description_cleaner(description)
        item_loader = ListingLoader(response=response)

        # Enforces rent between 0 and 40,000 please dont delete these lines
        if int(rent) <= 0 and int(rent) > 40000:
            return
        if rent is None:
            return
        landlord_name = response.css('div.widget.widget-background-white  p strong::text').get()
        landlord_phone = response.css('div.widget.widget-background-white  p::text').getall()[2].strip()
        landlord_email = response.css('div.widget.widget-background-white  p a::text').get()
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
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format
        get_amenities(description, amenties, item_loader)
        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
