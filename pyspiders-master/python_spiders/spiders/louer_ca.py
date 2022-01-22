# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import scrapy
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re
import json


class LouerSpider(scrapy.Spider):

    name = "louer"
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1
    #start_urls = ['https://www.louer.ca/appartement+condo+maison-a-louer-lanaudiere-villes/']
    start_urls = ['https://www.louer.ca/appartement+condo+maison+loft+maisons-de-ville+co-location-a-louer-montreal-villes/',
            #'https://www.louer.ca/appartement+condo+maison+loft+maisons-de-ville+co-location-a-louer-capitale-nationale-villes/',
            #'https://www.louer.ca/appartement+condo+maison+loft+maisons-de-ville+co-location-a-louer-beauce-villes/',
            #'https://www.louer.ca/appartement+condo+maison+loft+maisons-de-ville+co-location-a-louer-estrie-villes/',
            #'https://www.louer.ca/appartement+condo+maison-a-louer-lanaudiere-villes/',
            #'https://www.louer.ca/appartement+condo+maison+loft+maisons-de-ville+co-location-a-louer-laurentides-villes/',
            #'https://www.louer.ca/appartement+condo+maison-a-louer-laval-villes/',
            #'https://www.louer.ca/appartement+condo+maison+loft+maisons-de-ville+co-location-a-louer-monteregie-rive-sud-villes/',
            'https://www.louer.ca/appartement+condo+maison+loft+maisons-de-ville+co-location-a-louer-outaouais-villes/']
  
    # 1. SCRAPING level 1
    def parse(self,response):

        for i in range(1,2):
            
            apartments = Selector(requests.get(str(str(response.url)+'/'+str(i))).text).css('.result_row')
            
            for apartment in apartments:

                title = apartment.css(".item-name em::text").get()
                rex = re.findall(
                    r'\d+', apartment.css(".property-features li.price span::text").get())
                if len(rex) > 2:
                    continue
                if len(rex) > 1:
                    if int(rex[0]) == 0:
                        rent = rex[1]
                    else:
                        rent = int((int(rex[0])+int(rex[1]))/2)
                else:
                    rent = int(rex[0])

                rex = re.search(
                    r'\d+', apartment.css(".property-features li:contains('Chambres') span::text").get())
                room_count = '1'
                if rex:
                    room_count = rex[0]
                available_date = apartment.css(
                    ".property-features li:contains('Date') span::text").get()
                external_link = apartment.css(
                    ".row.dd1-btn-wrapper .go_details a::attr(href)").get()



                r = Selector(requests.get(external_link).text)


                bathroom_count = "".join(r.css(".property-features li:contains('bain') *::text").getall())
                if bathroom_count:
                    bathroom_count = re.search(r'\d+', bathroom_count)[0]

                square_meters = r.css(
                    ".dd2-all-features li:contains('urface') span::text").get()
                if square_meters:
                    square_meters = re.search(r'\d+', square_meters)[0]

                description = remove_white_spaces(
                    "".join(r.xpath("//*[@id='home']/span//text()").getall()))
                description = re.sub(
                    r'call.+|email.+|contact.+|apply.+|\d+.\d+.\d+.\d+', "", description.lower())

                if not square_meters:
                    rex = re.search(r'(\d+).à.(\d+).pi|(\d+).pi',description)
                    try:
                        if rex and len(rex.groups())>1:
                            square_meters = str(int((int(rex.groups()[0])+int(rex.groups()[1]))/2))
                        elif rex:
                            square_meters = str(int(rex.groups()[0]))
                    except:
                        pass

                


                images = r.css(".gallery-item::attr('data-src')").getall()
                images = ['https://www.louer.ca' + x for x in images]

                address = remove_white_spaces(
                    "".join(r.css("h5.panel-title *::text").getall()))
                longitude, latitude = '',''
                zipcode, city, addres='','',''
                try:
                    longitude, latitude = extract_location_from_address(address)
                    zipcode, city, addres = extract_location_from_coordinates(
                        longitude, latitude)
                except:
                    pass
                

                landlord_phone = r.css(".c-phone::text").get()
                landlord_name = r.css(".c-name::text").get()
                landlord_emails = r.css(".c-name::text").getall()
                landlord_email=''
                for e in landlord_emails:
                    if '@' in e:
                        landlord_email = e
                external_id = r.css("h1.panel-title::text").get()
                if external_id and '#' in external_id:
                    external_id = external_id.split('#')[-1]
                else:
                    external_id = ''

                property_type = 'apartment' if 'appartment' in description.lower(
                ) or 'appartment' in title.lower() or 'condo' in title.lower() else 'house'

                if int(rent) > 0 and int(rent) < 20000:
                    item_loader = ListingLoader(response=response)

                    # # MetaData
                    item_loader.add_value("external_link", external_link)  # String
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
                    # item_loader.add_value("bathroom_count", bathroom_count)  # Int

                    item_loader.add_value("available_date", available_date)

                    self.get_features_from_description(
                        description+" ".join(r.css(".dd2-all-features li *::text").getall()), r, item_loader)

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
                        "landlord_name", landlord_name)  # String
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
