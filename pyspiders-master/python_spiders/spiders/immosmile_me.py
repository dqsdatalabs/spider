# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import scrapy
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re


class DawoniaSpider(scrapy.Spider):

    name = "immosmile_me"
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for i in range(1,15):
            url = f'https://www.immosmile.me/suchergebnisse-mieten/objekttyp_kaufen/haus/objekttyp_mieten/wohnung.html?page={i}#object_list'
            yield Request(url, dont_filter=True, callback=self.parse)


    # 2. SCRAPING level 2

    def parse(self, response):
        apartments = response.css(".button_more a::attr(href)").getall()
        apartments = ['https://www.immosmile.me/'+x for x in apartments]
        for url in apartments:
            yield Request(url, callback=self.parseApartment)

    def parseApartment(self, response):

        title = remove_white_spaces("".join(response.css("h1.headline *::text").getall()))
        external_id = response.css(".item.objekt .text::text").get()
        property_type='apartment'
        if response.css(".item.objekt .value:contains(Wohnung)"):
            property_type = 'apartment'
        else:
            'house'

        square_meters = response.css(".item.flaechen:contains(Wohnfläche) .value::text").get()
        if square_meters:
            square_meters = re.search(r'\d+',square_meters)[0]

        room_count = response.css(".item.infos:contains(Zimmer) .text::text").get()
        if not room_count or room_count[0]=='0':
            room_count='1'

        floor = response.css(".item.infos:contains(Etage) .text::text").get()

        rent=response.css(".item.kosten:contains(Warmmiete) .value::text").get()
        if rent:
            rent = re.search(r'\d+', rent.replace('.', ''))[0]

        deposit=response.css(".item.kosten:contains(Kaution) .value::text").get()
        if deposit:
            deposit = re.search(r'\d+', deposit.replace('.', ''))[0]

        utilities = response.css(".item.kosten:contains(Nebenkosten) .value::text").get()
        if utilities:
            utilities = re.search(r'\d+', utilities.replace('.', ''))[0]

        energy_label = response.css(".item.energie:contains(Wertklasse) .value *::text").get()
        if energy_label:
            energy_label = energy_label.replace('\t','').replace('\n','')


        description = description_cleaner(" ".join(response.css(".object_info_item.texts.objektbeschreibung .value::text").getall()))
        Amenty = " ".join(response.css(".object_info_item.texts.ausstattung_freitext .value *::text").getall())
        location = extract_coordinates_regex(response.css("script:contains(LatLng)").get())
        latitude = str(location[0])
        longitude=str(location[1])
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        images = response.css(".cycle-slideshow-container img::attr(src)").getall()
        images = ['https://www.immosmile.me/'+x for x in images]

        if int(rent) > 0 and int(rent) < 20000:
            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value("external_link", response.url)  # String
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
            item_loader.add_value("floor", floor)  # String
            item_loader.add_value("property_type", property_type)  # String
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", int(float(room_count)))  # Int
            # item_loader.add_value("bathroom_count", bathroom_count)  # Int

            #item_loader.add_value("available_date", available_date)

            get_amenities(description,Amenty,item_loader)

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value(
                "external_images_count", len(images))  # Int
            #item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            item_loader.add_value("deposit", deposit)  # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities)  # Int
            item_loader.add_value("currency", "EUR")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            #item_loader.add_value("heating_cost", heating_cost) # Int

            item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            item_loader.add_value(
                "landlord_name", 'Immosmile')  # String
            item_loader.add_value(
                "landlord_phone", '+49 371 444 789 75')  # String
            item_loader.add_value(
                "landlord_email", 'hello@immosmile.me')  # String

            self.position += 1
            yield item_loader.load_item()

    Amenties = {
        'pets_allowed': ['pet'],
        'furnished': ['furnish', 'MÖBLIERTES'.lower()],
        'parking': ['parking', 'garage', 'parcheggio'],
        'elevator': ['elevator', 'aufzug', 'ascenseur', 'lift', 'aufzüg'],
        'balcony': ['balcon', 'balkon'],
        'terrace': ['terrace', 'terrazz', 'terras', 'terrass'],
        'swimming_pool': ['pool', 'piscine','schwimmbad'],
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
