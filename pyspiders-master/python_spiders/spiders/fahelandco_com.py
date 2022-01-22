# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import json
import scrapy
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re


class FahelandcoSpider(scrapy.Spider):

    name = "fahelandco"
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):

        urls = ['https://api.lethub.co/public/unit/list?slug=Fahel-and-Co&offset=0&limit=16&filter=default&q=']
        for url in urls:
            yield Request(url,
                          callback=self.parse,
                          dont_filter=True)

    # 2. SCRAPING level 2

    def parse(self, response):

        apartments = json.loads(requests.get(response.url).text)['units']
        #apartments = json.loads(response.body)['units']
        for apartment in apartments:
            external_id = str(apartment['propertyId'])
            imagesID = apartment['id']
            rent = apartment['rent']
            deposit=apartment['deposit']
            title = apartment['title']
            room_count = int(float(apartment['bed']))
            bathroom_count = int(float(apartment['bath']))
            AmentiesTxt = " ".join(apartment['amenities'])
            if apartment['parking']:
                AmentiesTxt+=" parking"
            if apartment['petFriendly']:
                AmentiesTxt+="pets"

            landlord_email = apartment['email']
            landlord_name = "Fahelandco"
            landlord_phone = apartment['phone']
            address = apartment['address']
            title = 'Apartment at ' + address
           
            
            longitude, latitude = '', ''
            zipcode, city, addres = '', '', ''
            try:
                longitude, latitude = extract_location_from_address(address)
                zipcode, city, addres = extract_location_from_coordinates(
                    longitude, latitude)
            except:
                pass
            description = apartment['description']
            description = re.sub("</?[a-z]+>", "",description)
            available_ = re.search(r'available as of [\w]+.[\w]+',description.lower())
            if available_:
                available_date = available_[0]
            if not available_ and 'available today' in description.lower():
                available_date = 'available today'
            description = re.sub(
            r'email.+|call.+|contact.+|apply.+|\d+.\d+.\d+.\d+', "", description.lower())

            rq = json.loads(requests.get(f"https://api.lethub.co/public/file/list?entityName=unit&entityId={imagesID}").text)
            
            images=[]
            for img in rq['files']:
                images.append(img['url'])

            if int(rent) > 0 and int(rent) < 20000:
                item_loader = ListingLoader(response=response)

                # # MetaData
                item_loader.add_value("external_link", "https://fahelandco.com/listing"+'#'+str(self.position))  # String
                item_loader.add_value(
                    "external_source", self.external_source)  # String

                item_loader.add_value("external_id", external_id)  # String
                item_loader.add_value("position", self.position)  # Int
                item_loader.add_value("title", title)  # String
                #item_loader.add_value("description", description)  # String

                # # Property Details
                item_loader.add_value("city", city)  # String
                item_loader.add_value("zipcode", zipcode)  # String
                item_loader.add_value("address", address)  # String
                item_loader.add_value("latitude", str(latitude))  # String
                item_loader.add_value("longitude", str(longitude))  # String
                # item_loader.add_value("floor", floor)  # String
                item_loader.add_value("property_type", 'apartment')  # String
                #item_loader.add_value("square_meters", square_meters)  # Int
                item_loader.add_value("room_count", room_count)  # Int
                item_loader.add_value("bathroom_count", bathroom_count)  # Int

                item_loader.add_value("available_date", available_date)

                self.get_features_from_description(
                    description+" "+AmentiesTxt, None, item_loader)

                # # Images
                item_loader.add_value("images", images)  # Array
                item_loader.add_value(
                    "external_images_count", len(images))  # Int
                # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

                # # Monetary Status
                item_loader.add_value("rent", rent)  # Int
                item_loader.add_value("deposit", deposit)  # Int
                # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                # item_loader.add_value("utilities", utilities)  # Int
                item_loader.add_value("currency", "CAD")  # String

                # item_loader.add_value("water_cost", water_cost) # Int
                # item_loader.add_value("heating_cost", heating_cost) # Int

                # item_loader.add_value("energy_label", energy_label)  # String

                # # LandLord Details
                item_loader.add_value(
                    "landlord_name",landlord_name)  # String
                item_loader.add_value(
                    "landlord_phone", landlord_phone)  # String
                item_loader.add_value("landlord_email", landlord_email)  # String

                self.position += 1
                yield item_loader.load_item()

    Amenties = {
        'pets_allowed': ['pet'],
        'furnished': ['furnish', 'MÖBLIERTES'.lower()],
        'parking': ['parking', 'garage', 'parcheggio'],
        'elevator': ['elevator', 'aufzug', 'ascenseur'],
        'balcony': ['balcon', 'balkon'],
        'terrace': ['terrace', 'terrazz', 'terras'],
        'swimming_pool': ['pool', 'piscine'],
        'washing_machine': [' washer', 'laundry', 'washing_machine', 'waschmaschine', 'laveuse'],
        'dishwasher': ['dishwasher', 'geschirrspüler', 'lave-vaiselle', 'lave vaiselle']
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
