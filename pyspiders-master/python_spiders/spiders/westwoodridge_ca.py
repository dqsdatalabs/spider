# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import scrapy
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
from urllib.parse import urlparse, urlunparse, parse_qs
import re


class WestwoodridgeSpider(scrapy.Spider):

    name = "westwoodridge"
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1
    handle_httpstatus_list = [404]
    # 1. SCRAPING level 1
    def start_requests(self):

        urls = ['https://www.westwoodridge.ca/listings/']
        for url in urls:
            yield Request(url,
                          callback=self.parse,
                          dont_filter=True)

    # 2. SCRAPING level 2

    def parse(self, response):

        apartments = response.css(".sp-media a::attr(href)").getall()
        apartments = ['https://www.westwoodridge.ca'+x for x in apartments]
        for url in apartments:
            yield Request(url, dont_filter=True,headers={
                'Accept':'*/*',
                'Accept-Encoding':'gzip, deflate, br',
                'Connection':'Keep-Alive',
                'User-Agent':"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36"},
                 callback=self.parseApartment)

    def parseApartment(self, response):
        title = response.css(".sp-page-title::text").get()
        rex = re.search(
            r'\d+', response.css(".sp-property-metric-rent::text").get().replace(',',''))
        rent = ''
        if rex:
            rent = rex[0]

        rex = re.search(
            r'\d+', response.css(".sp-property-metric-sqft::text").get().replace(',',''))
        square_meters = ''
        if rex:
            square_meters = rex[0]

        rex = re.search(
            r'\d+', response.css(".sp-property-metric-bedrooms::text").get())
        room_count = '1'
        if rex:
            room_count = rex[0]

        rex = re.search(
            r'\d+', response.css(".sp-property-metric-bathrooms::text").get())
        bathroom_count = '1'
        if rex:
            bathroom_count = rex[0]

        available_date = remove_white_spaces(response.css(".sp-property-metric-available_date::text").get())

        description = remove_white_spaces(
            "".join(response.css(".sp-property-details .sp-editor-content *::text").getall()))
        description = re.sub(
            r'email.+|call.+|contact.+|apply.+|\d+.\d+.\d+.\d+', "", description.lower())

        images = response.css(".sp-photo a::attr(href)").getall()
        images = [f"https://www.westwoodridge.ca/sp-datastore/cache/salesforce/attachments/{x.split('/')[-1]}" for x in images]

        r = Selector(requests.get(f"https://www.westwoodridge.ca/listing/{response.url.split('/')[-2]}/floorplan").text)
        floor_plan_images = r.css(".sp-property-floorplan a::attr(href)").getall()
        floor_plan_images = ['https://www.westwoodridge.ca'+x for x in floor_plan_images]

        
        r = Selector(requests.get(f"https://www.westwoodridge.ca/listing/{response.url.split('/')[-2]}/map").text)
        address = r.css(".sp-property-pane iframe::attr('src')").get()
        address = parse_qs(urlparse(address).query)["q"][0]

  
        longitude, latitude = '', ''
        zipcode, city, addres = '', '', ''
        try:
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, addres = extract_location_from_coordinates(
                longitude, latitude)
        except:
            pass

        
        property_type = 'apartment'
        

        if int(rent) > 0 and int(rent) < 20000:
            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value("external_link", response.url)  # String
            item_loader.add_value(
                "external_source", self.external_source)  # String

            #item_loader.add_value("external_id", external_id)  # String
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
                description+" ".join(response.css(".dd2-all-features li *::text").getall()), response, item_loader)

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value(
                "external_images_count", len(images))  # Int
            item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

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
                "landlord_name", 'Krown property managment')  # String
            item_loader.add_value(
                "landlord_phone", '709-738-6474')  # String
            # item_loader.add_value("landlord_email", landlord_email)  # String

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
