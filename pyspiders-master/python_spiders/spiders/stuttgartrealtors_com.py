# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import scrapy
from scrapy.http.request import Request

from python_spiders.loaders import ListingLoader

from ..helper import *
import re
import json


class StuttgartrealtorsSpider(scrapy.Spider):

    name = "stuttgartrealtors"
    start_urls = [
        'https://www.stuttgartrealtors.com/rentals.xhtml?f[2050-9]=miete']
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield Request(url,
                callback=self.parse,
                dont_filter=True)

    # 2. SCRAPING level 2

    def parse(self, response):
        pages = response.css('.anzahl-seiten:nth-child(1) span a::attr(href)').getall()
        for page in pages:
            yield Request('https://www.stuttgartrealtors.com/'+page,callback=self.parseApartment,dont_filter=True)

    def parseApartment(self, response):
   
        apartments = response.css('.listobject')
        for apartment in apartments:
            if 'rented' in apartment.css("li span::attr(style)").get() or 'reserved' in apartment.css("li span::attr(style)").get():
                continue
            title = apartment.css('h2 a::text').get()

            url = 'https://www.stuttgartrealtors.com/' + \
                apartment.css('h2 a::attr(href)').get()
            external_id =re.search(r'\d+',apartment.css(".listobject-objectnumber").get())[0]
            property_type = 'apartment' if 'partment' in apartment.css(".listobject-information tr:contains(Property) span::text").get() else 'house'

            rent = apartment.css(".listobject-information tr:contains(rent) span::text").get()
            if rent:
                rent = re.search(
                    r'\d+', rent.replace(',00', '').replace('.', ''))[0]
            else:
                continue

            square_meters = apartment.css(".listobject-information tr:contains(area) span::text").get()
            if square_meters:
                square_meters = re.search(
                    r'\d+', square_meters)[0]

            datausage = {
                'title': title,
                'rent': rent,
                'external_id': external_id,
                'property_type': property_type,
                'square_meters': square_meters,
            }


            yield Request(url, meta=datausage, callback=self.populate_item, dont_filter=True)

        return

    # 3. SCRAPING level 3

    def populate_item(self, response):
        
        title = response.meta['title']
        rent = response.meta['rent']
        external_id = response.meta['external_id']
        property_type = response.meta['property_type']
        square_meters = response.meta['square_meters']

        td = response.css(".objectdetails-details tr:contains(bathroom) td *::text").getall()
        bathroom_count = ''
        room_count = ''
        for i,t in enumerate(td):
            if 'bathroom' in t:
                bathroom_count = td[i+1][0]
            
        td = response.css(".objectdetails-details tr:contains(bedrooms) td *::text").getall()
        for i,t in enumerate(td):
            if 'bedrooms' in t:
                room_count = td[i+1][0]
        if room_count=='':
            room_count='1'
        td = response.css(".objectdetails-details tr:contains(tilities) td *::text").getall()
        utilities=''
        for i,t in enumerate(td):
            if 'tilities' in t:
                utilities = td[i+1]
        

        latlng = "".join(response.css('.objectdetails-googlemaps script::text').getall())
        location = extract_coordinates_regex(latlng)
        latitude = str(location[0])
        longitude = str(location[1])
        zipcode, city, address = extract_location_from_coordinates(longitude,latitude)

        description = remove_white_spaces(
            "".join(response.css(".objectdetails-information span *::text").getall()))


        images = response.css('.objectdetails-image a::attr(href)').getall()
        images = [x.replace('@800x600','') for x in images]


        
      ######################################
        if int(rent) > 0 and int(rent) < 20000:
            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value("external_link", response.url)  # String
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

            # String => date_format also "Available", "Available Now" ARE allowed
            #item_loader.add_value("available_date", available_date)

            self.get_features_from_description(
                description+" ".join(response.css(".objectdetails-details tr td *::text").getall()), response, item_loader)

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities)  # Int
            item_loader.add_value("currency", "EUR")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            #item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            item_loader.add_value(
                "landlord_name", 'Mr. Marc Schindler')  # String
            item_loader.add_value(
                "landlord_phone", '01727313100')  # String
            item_loader.add_value(
                "landlord_email", 'schindler1910@aol.com')  # String

            self.position += 1
            yield item_loader.load_item()

    def get_features_from_description(self, description, response, item_loader):
        description = description.lower()
        pets_allowed = 'pets' in description 
        furnished = 'furnish' in description 
        parking = 'parking' in description
        elevator = 'elevator' in description
        balcony = 'balcon' in description
        terrace = 'terrace' in description
        swimming_pool = 'pool' in description
        washing_machine = 'laundry' in description
        dishwasher = 'dishwasher' in description

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

    def get_property_type(self, property_type, description):

        if property_type and ('appartamento' in property_type.lower() or 'appartamento' in description.lower()):
            property_type = 'apartment'
        elif property_type and 'ufficio' in property_type.lower():
            property_type = ""
        else:
            if not property_type:
                property_type = ''
            else:
                property_type = 'house'
        return property_type
