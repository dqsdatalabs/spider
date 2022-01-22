# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import json
import scrapy
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re


class MySpider(scrapy.Spider):

    name = "albertaweidner"
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):

        urls = [
            'https://alberta.weidner.com/searchlisting.aspx?ftst=&txtDistance=25&LocationGeoId=0&zoom=10&autoCompleteCorpPropSearchlen=3&renewpg=1&LatLng=(37.09024,-95.712891)&']
        for url in urls:
            yield Request(url,
                          callback=self.parse,
                          dont_filter=True)

    # 2. SCRAPING level 2

    def parse(self, response):

        apartments = response.css('.box-text.row-fluid')
        for apartment in apartments:

            title = apartment.css(".propertyUrl::text").get()
            external_link = apartment.css(".propertyUrl::attr(href)").get()
            address = apartment.css(".propertyAddress::text").get()
            city = apartment.css(".propertyCity::text").get()
            #state = apartment.css("propertyState::text").get()
            zipcode = apartment.css(".propertyZipCode::text").get()
            address = str(address)+' '+str(city)+', '+str(zipcode)
            longitude, latitude = '', ''
            try:
                longitude, latitude = extract_location_from_address(address)
                longitude = str(longitude)
                latitude = str(latitude)
            except:
                pass

            landlord_phone = apartment.css(".prop-call-us::text").get()

            description = remove_white_spaces(
                " ".join(Selector(requests.get(external_link).text).css(".fade-content *::text").getall()))
            description = re.sub(
                r'email.+|call.+|contact.+|apply.+|\d+.\d+.\d+.\d+', "", description.lower())

            images = Selector(requests.get(external_link.replace('index.aspx', 'photo-gallery.aspx')).text).css(".item img::attr('data-src')").getall()
            amenities = " ".join(Selector(requests.get(external_link.replace(
                'index.aspx', 'amenities.aspx')).text).css(".pull-left::text").getall())
            floor_url = external_link.replace('index.aspx', 'floorplans')
            floor_plans = re.findall(r'urlname: "([\s?\w]+)"', (Selector(
                requests.get(floor_url).text).css("script:contains('baths')").getall()[2]))
            i = 0
            for floor in floor_plans:
                plan = floor_url+'/'+floor.replace(' ', '-')
                unit = Selector(requests.get(plan).text)

                rex = re.search(r'\d+', "".join(unit.css(
                    ".single-fp-flexcontainer:nth-child(1) .single-fp-flexitem:contains('ed')::text").getall()))
                room_count = '1'
                if rex:
                    room_count = rex[0]
                if not rex or room_count == '0':
                    room_count = '1'

                rex = re.search(r'\d+', "".join(unit.css(
                    ".single-fp-flexcontainer:nth-child(1) .single-fp-flexitem:contains('ath')::text").getall()))
                bathroom_count = '1'
                if rex:
                    bathroom_count = rex[0]
                if not rex or bathroom_count == '0':
                    bathroom_count = '1'

                rex = re.search(r'\d+', "".join(unit.css(
                    ".single-fp-flexcontainer:nth-child(1) .single-fp-flexitem:contains('q')::text").getall()))
                square_meters = '0'
                if rex:
                    square_meters = rex[0]
                if not rex or square_meters == '0':
                    square_meters = '0'
                floor_plan_images = []
                try:
                    floor_plan_images = 'https://cdngeneral.rentcafe.com' + \
                        unit.css(".main_engage_photo img::attr(src)").get()

                except:
                    pass

                property_type = 'apartment' if 'appartment' in description.lower(
                ) or 'appartment' in title.lower() else 'house'

                dataUsage = {
                    "property_type": property_type,
                    'title': title,
                    "amenities": amenities,
                    "external_link": plan,
                    "city": city,
                    "address": address,
                    "zipcode": zipcode,
                    "longitude": longitude,
                    "latitude": latitude,
                    "square_meters": square_meters,
                    "room_count": room_count,
                    "bathroom_count": bathroom_count,
                    'landlord_phone': landlord_phone,
                    'floor_plan_images': floor_plan_images,
                    "images": images,
                    "description": description,
                }

                apply_btn = unit.css(".fp-availApt-Container")

                if apply_btn:
                    for unt in apply_btn:
                        external_id = unt.css(
                            "span:contains('#')::text").get().replace('#', '')
                        rent = unt.css("span:contains('$')::text").get()
                        if rent:
                            rent = re.search(r'\d+', rent.replace(',', ''))[0]
                        else:
                            rent = '0'

                        dataUsage['rent'] = rent
                        dataUsage['external_id'] = external_id
                        i += 1
                        dataUsage['external_link'] = plan+'#'+str(i)
                        yield Request(plan+'#'+str(i), meta=dataUsage, dont_filter=True, callback=self.populate)

                else:
                    rent = unit.css(".promoPrice::text").get()
                    if rent:
                        rent = re.search(r'\d+', rent.replace(',', ''))[0]
                    else:
                        rent = '0'
                    dataUsage['rent'] = rent
                    dataUsage['external_id'] = ''
                    dataUsage['external_link'] = plan
                    yield Request(plan, meta=dataUsage, dont_filter=True, callback=self.populate)

    def populate(self, response):
        property_type = response.meta["property_type"]
        title = response.meta['title']
        external_id = str(response.meta["external_id"])
        external_link = response.meta["external_link"]
        city = response.meta["city"]
        address = response.meta["address"]
        zipcode = response.meta["zipcode"]
        amenities = response.meta["amenities"]
        longitude = response.meta["longitude"]
        latitude = response.meta["latitude"]
        square_meters = response.meta["square_meters"]
        room_count = response.meta["room_count"]
        bathroom_count = response.meta["bathroom_count"]
        landlord_phone = response.meta['landlord_phone']
        floor_plan_images = response.meta['floor_plan_images']
        images = response.meta["images"]
        rent = response.meta["rent"]
        description = response.meta["description"]

        if int(rent) > 0 and int(rent) < 20000:
            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value("external_link", external_link)  # String
            item_loader.add_value(
                "external_source", self.external_source)  # String

            item_loader.add_value("external_id", str(external_id))  # String
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

            #item_loader.add_value("available_date", available_date)

            self.get_features_from_description(
                description+" "+amenities, response, item_loader)

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value(
                "external_images_count", len(images))  # Int
            item_loader.add_value("floor_plan_images",
                                  floor_plan_images)  # Array

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
                "landlord_name", 'Alberta Weidner')  # String
            item_loader.add_value(
                "landlord_phone", landlord_phone)  # String
            # item_loader.add_value("landlord_email", 'info@mysuttonpm.com')  # String

            self.position += 1
            yield item_loader.load_item()

    Amenties = {
        'pets_allowed': ['pet'],
        'furnished': ['furnish', 'MÖBLIERTES'.lower()],
        'parking': ['parking', 'garage', 'parcheggio', 'stationnement'],
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
