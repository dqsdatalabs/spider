# -*- coding: utf-8 -*-
# Author: A.Abbas

import scrapy

from ..helper import *
from ..loaders import ListingLoader


class DeweerdtDeSpider(scrapy.Spider):
    name = "deweerdt_de"
    start_urls = [
        'https://www.deweerdt.de/immobilien/wohnen/mieten/wohnungen?lat=&lng=&sort%5B0%5D=n%7Cd&city=&district=&radius=0',
        'https://www.deweerdt.de/immobilien/wohnen/mieten/wohnungen,o6?lat=&lng=&sort%5B0%5D=n%7Cd&city=&district=&radius=0',
    ]
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
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

        apartment_page_links = response.xpath('//a[@class="no-hover"]')
        yield from response.follow_all(apartment_page_links, self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        title = response.xpath('//div[@class="expose"]//h1//text()').get().split(',')[0]
        description = " ".join(response.xpath('//div[@class="expose"]/p[1]//text()').getall())

        latitude = response.xpath('/html/body/script[6]//text()').get().split('map", ')[-1].split(', "')[0].split(", ")[0]
        longitude = response.xpath('/html/body/script[6]//text()').get().split('map", ')[-1].split(', "')[0].split(", ")[1]

        zipcode, city, address = extract_location_from_coordinates(longitude,latitude)

        landlord_name = response.xpath('//div[@class="right-box-immo"]//p[1]//strong//text()').get()
        landlord_number = response.xpath('//div[@class="right-box-immo"]//p[2]//text()').get().replace('Telefon:', '')

        floor_plan_images = response.xpath('//div[@class="carousel slide"]//a[@title="Grundriss"]//@href').get()
        all_images = response.xpath(
            '//div[@class="carousel slide"]//a[@class="sc-media-gallery embed-responsive-item"]//@title').getall()  # all images path
        images = []
        for image in all_images:
            if image != "Grundriss":  # her write what title that you don't need to scrap like the image that have company logo on it or layout
                images.append(
                    response.xpath(
                        f'//div[@class="carousel slide"]//a[@title="{image}"]//@href').get())  # image path with specific title

        all_information = response.xpath('//table//text()').getall()  # type the xpath for the table or ul
        object_information = []
        details = response.xpath(
            '//div[@class="expose"]/p//text()').getall()  # type the xpath for the description or all the text in the page

        for item in all_information:
            position = all_information.index(item)
            all_information[position] = remove_white_spaces(item).replace(":", '').lower().replace(" zzgl. nk", '')
            if not item.isspace():
                object_information.append(item.strip().lower().replace(":", '').replace(" zzgl. nk", ''))

        amenities = " ".join(details).lower() + " ".join(object_information)

        external_id = None
        if "objekt id" in object_information:
            position = object_information.index("objekt id")
            external_id = (object_information[position + 1])
        elif "objekt-nr." in object_information:
            position = object_information.index("objekt-nr.")
            external_id = (object_information[position + 1])

        floor = None
        if "etage" in object_information:
            position = object_information.index("etage")
            floor = (object_information[position + 1])

        property_type = response.xpath('//div[@class="right-box-immo"]//h4//text()').getall()[1]

        if string_found(['wohnung', 'etagenwohnung'], property_type):
            property_type = 'apartment'
        elif string_found(['haus'], property_type):
            property_type = "house"

        square_meters = None
        if "wohnfläche ca." in object_information:
            position = object_information.index("wohnfläche ca.")
            square_meters = int(convert_to_numeric(extract_number_only(object_information[position + 1])))
        elif "wohnfläche" in object_information:
            position = object_information.index("wohnfläche")
            square_meters = int(convert_to_numeric(extract_number_only(object_information[position + 1])))

        room_count = 1
        if "zimmer" in object_information:
            position = object_information.index("zimmer")
            room_count = convert_to_numeric(extract_number_only(object_information[position + 1].replace('.', ',')))
            check_room_type = isinstance(room_count, float)
            if check_room_type:
                room_count += 0.5
        elif "anzahl der zimmer gesamt" in object_information:
            position = object_information.index("anzahl der zimmer gesamt")
            room_count = convert_to_numeric(extract_number_only(object_information[position + 1].replace('.', ',')))
            check_room_type = isinstance(room_count, float)
            if check_room_type:
                room_count += 0.5

        bathroom_count = None
        if "badezimmer" in object_information:
            position = object_information.index("badezimmer")
            bathroom_count = convert_to_numeric(extract_number_only(object_information[position + 1].replace('.', ',')))
            check_bathroom_type = isinstance(bathroom_count, float)
            if check_bathroom_type:
                bathroom_count += 0.5
        elif "anzahl der bäder" in object_information:
            position = object_information.index("anzahl der bäder")
            bathroom_count = convert_to_numeric(extract_number_only(object_information[position + 1].replace('.', ',')))
            check_bathroom_type = isinstance(bathroom_count, float)
            if check_bathroom_type:
                bathroom_count += 0.5
        elif string_found(['badezimmer', 'bad'], amenities):
            bathroom_count = 1

        available_date = None
        if "bezugsfrei ab" in object_information:
            position = object_information.index("bezugsfrei ab")
            available_date = extract_date(object_information[position + 1])
        elif "verfügbar ab" in object_information:
            position = object_information.index("verfügbar ab")
            available_date = extract_date(object_information[position + 1])

        rent = None
        if "kaltmiete" in object_information:
            position = object_information.index("kaltmiete")
            rent = object_information[position + 1].replace('.00', '')
        elif "miete" in object_information:
            position = object_information.index("miete")
            rent = extract_number_only(object_information[position + 1].replace('.00', '')).replace('.00', '')

        deposit = None
        if "kaution" in object_information:
            position = object_information.index("kaution")
            deposit = object_information[position + 1]
            if "monatsmieten kalt" in deposit:
                deposit = int(extract_number_only(rent)) * int(extract_number_only(deposit))

        utilities = None
        if "nebenkosten" in object_information:
            position = object_information.index("nebenkosten")
            utilities = object_information[position + 1].replace('.00', '')

        energy_label = None
        if "energie\xadeffizienz\xadklasse" in object_information:
            position = object_information.index("energie\xadeffizienz\xadklasse")
            energy_label = object_information[position + 1].upper()
        elif "energieeffizienzklasse" in object_information:
            position = object_information.index("energieeffizienzklasse")
            energy_label = object_information[position + 1].upper()

        pets_allowed = False
        if string_found(['Haustiere: Nach Vereinbarung', 'haustiere erlaubt? ja'], amenities):
            pets_allowed = True

        furnished = False
        if string_found(['möblierte', 'möbliert'], amenities):
            furnished = True

        parking = False
        if string_found(
                ['Tiefgarage', 'Stellplatz', 'Garage', 'Tiefgaragenstellplatz', 'Außenstellplatz', 'Stellplätze',
                 'Einzelgarage', 'besucherparkplatz', 'parkplatz'], amenities):
            parking = True

        elevator = False
        if string_found(['Aufzug', 'Aufzügen', 'Fahrstuhl', 'Personenaufzug', 'Personenfahrstuhl'], amenities):
            elevator = True

        balcony = False
        if string_found(['Balkone', "Balkon", 'Südbalkon'], amenities):
            balcony = True

        terrace = False
        if string_found(['Terrassenwohnung', 'Terrasse', 'Terrasse (ca.)', 'Dachterrasse', 'Südterrasse'], amenities):
            terrace = True

        washing_machine = False
        if string_found(['gemeinschaftlicher Wasch', 'Trockenraum', 'Waschküche', 'Waschmaschinenzugang'], amenities):
            washing_machine = True

        dishwasher = False
        if string_found(['Spülmaschine', 'Geschirrspüler'], amenities):
            dishwasher = True

        # # MetaData
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", remove_white_spaces(external_id))  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", remove_white_spaces(title))  # String
        item_loader.add_value("description", remove_white_spaces(description))  # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        item_loader.add_value("floor", floor)  # String
        item_loader.add_value("property_type", property_type)  # String
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", available_date)  # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        item_loader.add_value("furnished", furnished)  # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        item_loader.add_value("deposit", deposit)  # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", remove_white_spaces(landlord_name))  # String
        item_loader.add_value("landlord_phone", remove_white_spaces(landlord_number))  # String
        item_loader.add_value("landlord_email", 'info@deWeerdt.de')  # String

        self.position += 1
        yield item_loader.load_item()
