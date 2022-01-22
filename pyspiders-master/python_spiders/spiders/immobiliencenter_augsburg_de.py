# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name


import scrapy
from ..loaders import ListingLoader
from ..helper import *


class ImmobiliencenterAugsburgDeSpider(scrapy.Spider):
    name = "immobiliencenter_augsburg_de"
    start_urls = [
        'https://www.immobiliencenter-augsburg.de/index.php4?cmd=searchDetails&alias=suchmaske&obercmd=search_alias_suchmaske&icmd=14477796013078&objq[cursor]=0']
    allowed_domains = ["immobiliencenter-augsburg.de"]
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for num in range(50):
            url = self.start_urls[0][:-1] + str(num)
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # if the sell type not rent or price cell does not contain salary or rented word in statues cell
        if response.css('.exposepreis .key::text').get() != 'Kaltmiete' or '€' not in response.css(
                '.exposepreis .wert::text').get() or 'RESERVIERT' in response.css(
                '#objektbildcarousel span::text').extract() or 'VERMIETET' in response.css(
                '#objektbildcarousel span::text').extract():
            return

        # Images
        pre_url = 'https://www.immobiliencenter-augsburg.de/'
        raw_images = response.css('.focusview::attr(src)').extract()
        images = [pre_url + i for i in raw_images]

        # Lat, Lng
        longitude, latitude = extract_location_from_address(
            response.xpath('//*[@id="inhalt"]/div[3]/div/div[2]/div[1]/div/div/div[2]/text()').get())

        # zipcode, city, address
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        # Property type
        german_names = {
            'Etagenwohnung': 'apartment',
            'Dachgeschosswohnung': 'apartment',
            'Reihenmittelhaus': 'house',
            'Wohngrundstück': 'house',
            'Einfamilienhaus': 'house',
            'Doppelhaushälfte': 'house',
            'Penthousewohnung': 'house',
            'Zweifamilienhaus': 'house' }
        name = response.xpath('//*[@id="inhalt"]/div[3]/div/div[2]/div[1]/div/div/div[1]/text()').get()
        if name == 'Bürofläche' or name == 'Sonstiges':
            return
        property_type = german_names[name]

        # Contact info
        info = response.xpath('//*[@id="inhalt"]/div[4]/div/div/div[2]/div/div[1]/text()').extract()
        landlord_number = '0821 - 90 75 390'
        landlord_email = 'info@immobiliencenter-augsburg.de'
        landlord_name = response.css('.kontaktname::text').get()

        for i in info:
            if 'Telefon' in i:
                landlord_number = i.replace(' Telefon: ', '')

        mail_to = response.xpath('//*[@id="inhalt"]/div[4]/div/div/div[2]/div/div[1]/a/@href').extract()
        if len(mail_to) > 0:
            landlord_email = mail_to[0].replace('mailto:', '')

        # Title
        title = response.css('h1::text').get()

        # Description
        desc_list = response.css('p::text').extract()
        description = ''.join(desc_list)

        # Rent
        rent = int(float(response.css('.exposepreis .wert::text').get().replace('.','').split(',')[0]))

        # energy labels
        energy_label = None
        labels = response.css('#inhalt .col-sm-12.col-lg-3 .name::text').extract()
        values = response.css('#inhalt .col-sm-12.col-lg-3 .wert::text').extract()

        for i in range(len(labels)):
            if 'Energieausweis Werteklasse' == labels[i]:
                energy_label = values[i]

        # square meters
        square_meters = int(
            float(response.css('.col-sm-6:nth-child(2) .wert::text').get().split()[0].replace(',', '.')))

        # external id
        external_id = response.css('.wert a::text').get()

        # Property specs
        labels = response.css('.col-pt-6 .key::text').extract()
        values = response.css('.col-pt-6:nth-child(2) :nth-child(1)::text').extract()
        check_labels = ['Garage',
                        'Unterkellert',
                        'Gartennutzung',
                        'Abstellraum',
                        'Dachboden',
                        'Gäste-WC',
                        'Fahrradraum',
                        'barrierefrei',
                        'Heizkosten sind in Nebenkosten enthalten',  # heat_cost_include
                        'Haustiere',  # pet_allowed
                        'Seniorengerecht',
                        'Kamin',
                        'Swimmingpool',  # pool
                        'rollstuhlgerecht'
                        ]
        check_values = response.css('.col-pt-6 .fa::attr(class)').extract()
        exist_check_labels = []
        new_labels = []
        # pet_allowed, pool, heat_cost_include
        for i in labels:
            if i in check_labels:
                exist_check_labels.append(i)
            else:
                new_labels.append(i)

        pets_allowed = None
        swimming_pool = None
        heat_included = None
        for i in range(len(exist_check_labels)):
            if exist_check_labels[i] == 'Haustiere':
                if check_values[i] == 'fa fa-check':
                    pets_allowed = True
                else:
                    pets_allowed = False
            if exist_check_labels[i] == 'Swimmingpool':
                if check_values[i] == 'fa fa-check':
                    swimming_pool = True
                else:
                    swimming_pool = False
            if exist_check_labels[i] == 'Heizkosten sind in Nebenkosten enthalten':
                if check_values[i] == 'fa fa-check':
                    heat_included = True
                else:
                    heat_included = False

        # Available_Date, bathroom, floor, parks, deposit, balcony, terrace, elevator
        available_date = None
        bathroom_count = None
        deposit = None
        floor = None
        parking = None
        balcony = None
        terrace = None
        elevator = None
        extra_cost = None
        for i in range(len(new_labels)):
            if new_labels[i] == 'Nebenkosten (ca.)':
                extra_cost = int(float(values[i].replace(',', '').replace('€', '').replace('-','').replace('.','').strip()))
            if new_labels[i] == 'Etage':
                floor = values[i]
            if new_labels[i] == 'Badezimmer':
                bathroom_count = int(float(values[i]))
            if new_labels[i] == 'Kaution':
                deposit = int(float(values[i].replace(',', '').replace('€', '').replace('-','').replace('.','').strip()))
            if new_labels[i] == 'Größe Balkon / Terrasse (ca.)':
                balcony = True
                terrace = True
            if new_labels[i] == 'Aufzug':
                elevator = True
            if new_labels[i] == 'verfügbar ab':
                date = values[i].split('.')[::-1]
                available_date = '-'.join(date)
            if new_labels[i] == 'Anzahl Stellplätze' or new_labels[i] == 'Stellplatzart':
                parking = True

        # heat_cost
        heating_cost = None
        if heat_included:
            heating_cost = extra_cost

        # Furnished
        furnished = True
        if len(check_values)<1:
            furnished = False


        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String11

        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        item_loader.add_value("floor", floor)  # String
        item_loader.add_value("property_type",
                              property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", int(float(response.css('.col-sm-6:nth-child(3) .wert::text').get())))  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", available_date)  # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        item_loader.add_value("furnished", furnished)  # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
        # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        item_loader.add_value("deposit", deposit)  # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost)  # Int

        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email)  # String

        self.position += 1
        yield item_loader.load_item()
