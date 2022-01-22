# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address


class DeltabauPyspiderGermanySpider(scrapy.Spider):
    name = "DeltaBau"
    start_urls = ['https://www.deltabau.de/immobilienangebote/?tx_cfismanagement_pi6%5Baction%5D=index&tx_cfismanagement_pi6%5Bcontroller%5D=Searchform&tx_cfismanagement_pi6%5B%40widget_0%5D%5BcurrentPage%5D=1&cHash=651e23e43855f24f55e28e23af05c409']
    allowed_domains = ["deltabau.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    i = 1
    urls = ['https://www.deltabau.de/immobilienangebote/?tx_cfismanagement_pi6%5Baction%5D=index&tx_cfismanagement_pi6%5Bcontroller%5D=Searchform&tx_cfismanagement_pi6%5B%40widget_0%5D%5BcurrentPage%5D=1&cHash=651e23e43855f24f55e28e23af05c409']

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        total_result = response.css('section.pagination-row h2 strong::text')[0].extract()
        total_result = total_result.split()[0]
        total_result = int(total_result)
        total_result = int(total_result / 10) + (total_result % 10 > 0)
        next_page_div = response.css('section.pagination-row nav ul li.next')
        if self.i <= total_result-2:
            if len(next_page_div) >= 1:
                next_page = next_page_div.css('a::attr(href)').extract()
                next_page = 'https://www.deltabau.de/' + next_page[0]
                self.urls.append(next_page)
                yield scrapy.Request(next_page, callback=self.parse)
                self.i += 1
        else:
            for url in self.urls:
                yield scrapy.Request(url, callback=self.parse_pages, dont_filter=True)

    def parse_pages(self, response):
        apartments_divs = response.css('div.immobilienergebnisse section.propertylist div.real_estate')
        for apartment_div in apartments_divs:
            apartment_url = apartment_div.css('div.row div.content h3 a::attr(href)').extract()
            url = 'https://www.deltabau.de/' + apartment_url[0]
            title = apartment_div.css('div.row div.content h3 a::text').extract()
            available_date = apartment_div.css('div.row div.content div::text').extract()
            apartment_details = apartment_div.css('div.row div.content div.angaben div.box::text').extract()
            yield scrapy.Request(url, callback=self.populate_item, meta={
                'title': title,
                'available_date': available_date,
                'apartment_details': apartment_details,
            })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        title = response.meta.get('title')
        title = title[0]
        title = title.replace('\n', '')
        title = title.replace('\r', '')
        title = title.strip()

        available_date_all = response.meta.get('available_date')
        available_date = None
        for item in available_date_all:
            if "verfügbar ab" in item:
                available_date = item
        available_date = available_date.split(':')[1]
        available_date = " ".join(available_date.split())
        if 'sofort' in available_date.lower():
            available_date = None
        elif 'nach' in available_date.lower():
            available_date = None
        elif '.' in available_date:
            available_date = available_date.split(".")
            day = available_date[0]
            month = available_date[1]
            year = available_date[2]
            available_date = year + "-" + month + "-" + day
        else:
            day = '01'
            month = '01'
            year = available_date
            available_date = year + "-" + month + "-" + day

        apartment_details = response.meta.get('apartment_details')
        square_meters = None
        room_count = None
        rent = None
        for item in apartment_details:
            if 'Fläche:' in item:
                square_meters = item
                square_meters = " ".join(square_meters.split())
            if 'Miet-/Kaufpreis:' in item:
                rent = item
                rent = " ".join(rent.split())
            if 'Zimmer:' in item:
                room_count = item
                room_count = " ".join(room_count.split())

        if 'auf anfrage' not in rent.lower():
            if square_meters:
                square_meters = square_meters.split(':')[1]
                square_meters = square_meters.strip()
                square_meters = square_meters.split()[0]
                square_meters = square_meters.replace(".", "")
                square_meters = square_meters.replace(",", ".")
                square_meters = round(float(square_meters))
                square_meters = int(square_meters)

            if room_count:
                room_count = room_count.split(':')[1]
                room_count = room_count.strip()
                room_count = room_count.replace(",", ".")
                room_count = round(float(room_count))
                room_count = int(room_count)

            rent = rent.split(':')[1]
            rent = rent.strip()
            rent = rent.split()[0]
            rent = rent.replace(".", "")
            rent = rent.replace(",", ".")
            rent = round(float(rent))
            rent = int(rent)
            # Enforces rent between 0 and 40,000 please dont delete these lines
            if int(rent) <= 0 and int(rent) > 40000:
                return

            apartment_info = response.css('div.expose div.row div table tr')
            apartment_info_keys = apartment_info.xpath('.//td[1]/text()').extract()
            apartment_info_values = apartment_info.xpath('.//td[2]/text()').extract()
            apartment_info_dict = dict(zip(apartment_info_keys, apartment_info_values))
            if 'Bürotyp:' not in apartment_info_dict.keys():
                if apartment_info_dict['Vermarktungsart:'] != 'Kauf':
                    if 'Anlageimmobilie:' in apartment_info_dict.keys():
                        property_type = 'house'
                    else:
                        property_type = 'apartment'

                    address_div = response.css('div.expose div.row div')
                    address = address_div.xpath('.//p[1]/text()').extract()
                    address = " ".join(address)
                    address = " ".join(address.split())
                    zipcode = address.split()[-2]
                    city = address.split()[-1]
                    address = address.split()
                    if len(address) == 2:
                        address = " ".join(address)
                    else:
                        address = " ".join(address[:-2]) + ", " + " ".join(address[-2:])
                    address = address + ', Germany'

                    external_id = address_div.xpath('.//p[2]/text()')[0].extract()
                    external_id = external_id.split(':')[1]
                    external_id = external_id.strip()

                    images = response.css('div.expose div.row div div.immo-pint-grid div a::attr(href)').extract()

                    if not room_count:
                        if 'Zimmeranzahl:' in apartment_info_dict.keys():
                            room_count = apartment_info_dict['Zimmeranzahl:']
                            room_count = room_count.replace(",", ".")
                            room_count = round(float(room_count))
                            room_count = int(room_count)

                    if room_count:
                        item_loader = ListingLoader(response=response)
                        bathroom_count = None
                        parking = None
                        deposit = None
                        utilities = None
                        heating_cost = None
                        warm_rent = None
                        floor = None
                        balcony = None
                        if 'Anzahl Badezimmer:' in apartment_info_dict.keys():
                            bathroom_count = apartment_info_dict['Anzahl Badezimmer:']
                            bathroom_count = bathroom_count.replace(",", ".")
                            bathroom_count = round(float(bathroom_count))
                            bathroom_count = int(bathroom_count)
                        else:
                            if 'Gäste-WC:' in apartment_info_dict.keys():
                                if apartment_info_dict['Gäste-WC:'] == 'Ja':
                                    bathroom_count = 1

                        if 'Stellplatz-Miete:' in apartment_info_dict.keys():
                            parking = True

                        if 'Kaution:' in apartment_info_dict.keys():
                            deposit = apartment_info_dict['Kaution:']
                            deposit = deposit.split()[0]
                            deposit = deposit.replace(".", "")
                            deposit = deposit.replace(",", ".")
                            deposit = round(float(deposit))
                            deposit = int(deposit)

                        if 'Nebenkosten:' in apartment_info_dict.keys():
                            utilities = apartment_info_dict['Nebenkosten:']
                            utilities = utilities.split()[0]
                            utilities = utilities.replace(".", "")
                            utilities = utilities.replace(",", ".")
                            utilities = round(float(utilities))
                            utilities = int(utilities)

                        if 'Heizungskosten:' in apartment_info_dict.keys():
                            heating_cost = apartment_info_dict['Heizungskosten:']
                            heating_cost = heating_cost.split()[0]
                            heating_cost = heating_cost.replace(".", "")
                            heating_cost = heating_cost.replace(",", ".")
                            heating_cost = round(float(heating_cost))
                            heating_cost = int(heating_cost)

                        if 'Warmmiete:' in apartment_info_dict.keys():
                            warm_rent = apartment_info_dict['Warmmiete:']
                            warm_rent = warm_rent.split()[0]
                            warm_rent = warm_rent.replace(".", "")
                            warm_rent = warm_rent.replace(",", ".")
                            warm_rent = round(float(warm_rent))
                            warm_rent = int(warm_rent)

                        if warm_rent:
                            heating_cost = warm_rent - rent

                        description_keys = response.css('div.expose div.box h3::text').extract()
                        description_values = response.css('div.expose div.box p::text').extract()
                        description_dict = dict(zip(description_keys, description_values))
                        if 'Sonstiges' in description_dict.keys():
                            del description_dict['Sonstiges']
                        description = " ".join(description_dict.values())
                        description = " ".join(description.split())

                        floor_balcony = response.css('div.expose div.box div table tr')
                        floor_balcony_keys = floor_balcony.xpath('.//td[1]/text()').extract()
                        floor_balcony_values = floor_balcony.xpath('.//td[2]/text()').extract()
                        floor_balcony_dict = dict(zip(floor_balcony_keys, floor_balcony_values))
                        if 'Etage:' in floor_balcony_dict.keys():
                            floor = floor_balcony_dict['Etage:']
                        if 'Balkon oder Terrasse:' in floor_balcony_dict.keys():
                            balcony = True
                        if 'Garage oder Stellplatz:' in floor_balcony_dict.keys():
                            parking = True

                        landlord_info = response.css('div.contact.fixed-contact')
                        landlord_name = landlord_info.xpath('.//p[1]//strong/text()').extract()
                        landlord_name = " ".join(landlord_name)
                        landlord_name = " ".join(landlord_name.split())
                        landlord_number = landlord_info.xpath('.//p[3]//strong/text()')[0].extract()
                        if ":" in landlord_number:
                            landlord_number = landlord_number.split(':')[1]
                            landlord_number = landlord_number.strip()
                            if 'Array' in landlord_number:
                                landlord_number = '0511 280060'
                        landlord_email = 'kontakt@deltabau.de'

                        longitude, latitude = extract_location_from_address(address)
                        longitude = str(longitude)
                        latitude = str(latitude)

                        # # MetaData
                        item_loader.add_value("external_link", response.url) # String
                        item_loader.add_value("external_source", self.external_source) # String

                        item_loader.add_value("external_id", external_id) # String
                        item_loader.add_value("position", self.position) # Int
                        item_loader.add_value("title", title) # String
                        item_loader.add_value("description", description) # String

                        # # Property Details
                        item_loader.add_value("city", city) # String
                        item_loader.add_value("zipcode", zipcode) # String
                        item_loader.add_value("address", address) # String
                        item_loader.add_value("latitude", latitude) # String
                        item_loader.add_value("longitude", longitude) # String
                        item_loader.add_value("floor", floor) # String
                        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
                        item_loader.add_value("square_meters", square_meters) # Int
                        item_loader.add_value("room_count", room_count) # Int
                        item_loader.add_value("bathroom_count", bathroom_count) # Int

                        item_loader.add_value("available_date", available_date) # String => date_format

                        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                        #item_loader.add_value("furnished", furnished) # Boolean
                        item_loader.add_value("parking", parking) # Boolean
                        #item_loader.add_value("elevator", elevator) # Boolean
                        item_loader.add_value("balcony", balcony) # Boolean
                        #item_loader.add_value("terrace", terrace) # Boolean
                        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                        #item_loader.add_value("washing_machine", washing_machine) # Boolean
                        #item_loader.add_value("dishwasher", dishwasher) # Boolean

                        # # Images
                        item_loader.add_value("images", images) # Array
                        item_loader.add_value("external_images_count", len(images)) # Int
                        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

                        # # Monetary Status
                        item_loader.add_value("rent", rent) # Int
                        item_loader.add_value("deposit", deposit) # Int
                        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                        item_loader.add_value("utilities", utilities) # Int
                        item_loader.add_value("currency", "EUR") # String

                        #item_loader.add_value("water_cost", water_cost) # Int
                        item_loader.add_value("heating_cost", heating_cost) # Int

                        #item_loader.add_value("energy_label", energy_label) # String

                        # # LandLord Details
                        item_loader.add_value("landlord_name", landlord_name) # String
                        item_loader.add_value("landlord_phone", landlord_number) # String
                        item_loader.add_value("landlord_email", landlord_email) # String

                        self.position += 1
                        yield item_loader.load_item()
