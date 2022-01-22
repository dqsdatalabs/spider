# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import scrapy
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re
import json


class ApartmentfindSpider(scrapy.Spider):

    name = "apartmentfind"
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1


    # 1. SCRAPING level 1
    def start_requests(self):
        for page in range(1,52):
            url = f'https://apartmentfind.ca/category/rental-search-ottawa/page/{page}/'
            yield Request(url,
                callback=self.parse,
                dont_filter=True)

    # 2. SCRAPING level 2

    def parse(self, response):

        urls = response.css(".article-wrapper .entry-header a::attr(href)").getall()
        for url in urls:
            yield Request(url, dont_filter=True, callback=self.parseApartment)

    def parseApartment(self, response):

        if 'commercial' in response.url:
            return

        title = response.css("h1.entry-title::text").get()
        rent='0'
        external_id=''
        try:
            rent = re.search(r'\d+\$',title.replace(',',''))[0].replace('$','')
            external_id = re.search(r'#\d+',title)[0].replace('#','')
        except:
            pass

        images = response.css(".fg-item-inner a::attr(href)").getall()
        if len(images)==0:
            images = response.css("img.skip-lazy::attr('data-src')").getall()
        if len(images)==0:
            images = response.css(".fg-item-inner a img::attr('data-src-fg')").getall()
        if len(images)==0:
            images = response.css(".attachment-post-thumbnail::attr(src)").getall()
        txt = response.css(".entry-content h2 *::text,.entry-content h3 *::text").getall()

        description=''
        for t in txt:
            if len(t)>150:
                description=t
                break
        
        address = re.sub(r'\d+\$','',title.replace(' (',','))
        address = address[0:address.find(')')]
        longitude, latitude='',''
        zipcode, city='',''
        if len(address)>5:
            try:
                longitude, latitude = extract_location_from_address(address)
                zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
            except:
                pass
            
        if city=='' or not city:
            city='Ottawa'
        Ntxt = (" ".join(txt)).lower()
        room_count = '1'
        if re.search(r'\d+ bedroom',Ntxt):
            room_count=re.search(r'\d+ bedroom',Ntxt)[0]
        else:
            room_count='1'
        if not room_count[0].isdigit():
            room_count='1'

        try:
            if int(room_count)==0:
                room_count='1'
        except:
            room_count='1'
        
        bathroom_count = '1'
        if re.search(r'\d+ bathroom',Ntxt):
            bathroom_count=re.search(r'\d+ bathroom',Ntxt)[0]

        available_date=''
        if 'available' in Ntxt and 'now' in Ntxt:
            available_date = 'Available Now'

        square_meter = re.search(r'\d+.sqft',description.lower())
        square_meters=''
        if square_meter:
            square_meters=square_meter[0].replace('sqft','').replace(' ','')
        
        if '\[contact-form\]\[contact-field' in description:
            description=''
        
       
        property_type = 'apartment' if 'appartment' in description.lower(
        ) or 'appartment' in title.lower() or 'condo' in title.lower() else 'house'

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
            item_loader.add_value("latitude", str(latitude))  # String
            item_loader.add_value("longitude", str(longitude))  # String
            # item_loader.add_value("floor", floor)  # String
            item_loader.add_value("property_type", property_type)  # String
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            item_loader.add_value("available_date", available_date)

            self.get_features_from_description(
                description+Ntxt, response, item_loader)

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value(
                "external_images_count", len(images))  # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            # item_loader.add_value("deposit", deposit)  # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities)  # Int
            item_loader.add_value("currency", "CAD")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            item_loader.add_value(
                "landlord_name", 'Apartment Find')  # String
            item_loader.add_value(
                "landlord_phone", '613-845-0338')  # String
            item_loader.add_value("landlord_email", 'info@ApartmentFind.ca')  # String

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
            x in description for x in self.Amenties['pets_allowed']) and 'no pet ' not in description else False
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
