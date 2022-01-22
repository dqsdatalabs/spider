# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re
import json


class Rahat_apartmentsSpider(scrapy.Spider):

    name = "rahat_apartments"

    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        url = f'https://www.rahat-apartments.de/apartmentsuche/?city=Berlin&district=&available_at=&submit=Wohnung+finden'

        yield Request(url,
            callback=self.parseApartment, 
            dont_filter=True)


    # 2. SCRAPING level 2

    def parse(self, response):
        pass

    def parseApartment(self, response):

        apartments = response.css('.accommodation-block.clearfix ')
        for apartment in apartments:
            title = apartment.css(".accommodation-custom-subtitle h3::text").get()
            room_count = apartment.css(".features .left li:contains(immer)::text").get()[0]
            square_meters = apartment.css(".features .left li:contains(ca)::text").get()
            if square_meters:
                square_meters = re.search(r'\d+',square_meters)[0]
            available_date = apartment.css(".features .left li:contains(ab)::text").get().replace('ab ','')
            if 'sofort' in available_date:
                available_date = 'Available now'

            url = apartment.css("a::attr(href)").get()
            external_id = url.split('/')[-2]
            #property_type = response.meta['property_type']
            rent = apartment.css(".features .right .price h3::text").get()
            if rent:
                rent = re.search(
                    r'\d+', rent.replace(',00', '').replace('.', ''))[0]
            else:
                continue

            datausage = {
                'title': title,
                'room_count': room_count,
                'rent': rent,
                'external_id': external_id,
                'square_meters': square_meters,
                'available_date': available_date,
            }

            yield Request(url, meta=datausage, callback=self.populate_item, dont_filter=True)

    # 3. SCRAPING level 3

    def populate_item(self, response):

        title = response.meta['title']
        external_id = response.meta['external_id']
        room_count = response.meta['room_count']
        rent = response.meta['rent']
        square_meters = response.meta['square_meters']
        available_date = response.meta['available_date']




        latlng = response.css("script:contains(gmpAllMapsInfo)").get()
        location = extract_coordinates_regex(latlng)
        latitude = str(location[0])
        longitude = str(location[1])
        zipcode, city, address = extract_location_from_coordinates(longitude,latitude)

        dText = response.css(".wpb_wrapper p::text").getall()
        for txt in dText:
            if len(txt)>150:
                description = txt
        #description = remove_white_spaces(
        #    "".join(response.xpath("/html/body/div[1]/div[3]/div/div[5]/div/div/div/div[3]/div/p//text()").getall())).replace('?', '').lower()
       

   
        #bathroom_count = re.search(r'\d+ bathroom', description)
       

        images = response.css(".rev_slider li::attr('data-thumb')").getall()
        images = [re.sub(r'\-\d+x\d+','',x) for x in images]

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
            # item_loader.add_value("floor", floor)  # String
            item_loader.add_value("property_type", 'apartment')  # String
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            #item_loader.add_value("bathroom_count", bathroom_count)  # Int

            item_loader.add_value("available_date", available_date)

            self.get_features_from_description(
                description+" ".join(response.css(".wpb_wrapper ul li::text,.wpb_wrapper ul li span::text").getall()), response, item_loader)

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            #item_loader.add_value("utilities", utilities)  # Int
            item_loader.add_value("currency", "EUR")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            item_loader.add_value(
                "landlord_name", 'Rahat Apartments Berlin Charlottenhöfe')  # String
            item_loader.add_value(
                "landlord_phone", '+49(0)30 8891 7440')  # String
            item_loader.add_value(
                "landlord_email", 'info@rahat-apartments.de')  # String

            self.position += 1
            yield item_loader.load_item()

    
    Amenties = {
        'pets_allowed':['pets'],
        'furnished':['furnish','MÖBLIERTES'.lower()],
        'parking':['parking','garage'],
        'elevator':['elevator','aufzug'],
        'balcony':['balcon','balkon'],
        'terrace':['terrace'],
        'swimming_pool':['pool'],
        'washing_machine':[' washer','laundry','washing_machine','waschmaschine'],
        'dishwasher':['dishwasher','geschirrspüler']
    }

    def get_features_from_description(self, description, response, item_loader):
        description     = description.lower()
        pets_allowed    = True if any(x in description for x in self.Amenties['pets_allowed']) else False
        furnished       = True if any(x in description for x in self.Amenties['furnished']) else False
        parking         = True if any(x in description for x in self.Amenties['parking']) else False
        elevator        = True if any(x in description for x in self.Amenties['elevator']) and 'ohne aufzug'not in description else False
        balcony         = True if any(x in description for x in self.Amenties['balcony']) else False
        terrace         = True if any(x in description for x in self.Amenties['terrace']) else False
        swimming_pool   = True if any(x in description for x in self.Amenties['swimming_pool']) else False
        washing_machine = True if any(x in description for x in self.Amenties['washing_machine']) else False
        dishwasher      = True if any(x in description for x in self.Amenties['dishwasher']) else False

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