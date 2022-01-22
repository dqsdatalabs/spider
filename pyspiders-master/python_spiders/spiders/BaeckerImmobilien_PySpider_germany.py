# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address


class BaeckerimmobilienPyspiderGermanySpider(scrapy.Spider):
    name = "BaeckerImmobilien"
    start_urls = ['https://www.baecker-immobilien.de/miete.xhtml']
    allowed_domains = ["baecker-immobilien.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
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
        pages_number = response.css('p.center span a::text')[-1].extract()
        pages_number = int(pages_number)
        urls = ['https://www.baecker-immobilien.de/miete.xhtml']
        for i in range(pages_number-1):
            page_url = 'https://www.baecker-immobilien.de/miete.xhtml?s[38133-411]=1%22%29&p[obj0]=' + str(i+2)
            urls.append(page_url)
        for url in urls:
            yield scrapy.Request(url, callback=self.parse_pages, dont_filter=True)

    def parse_pages(self, response):
        apartments_divs = response.css('div.list-object')
        for apartment_div in apartments_divs:
            property_type = apartment_div.css('input.object_art::attr(value)')[0].extract()
            property_type = property_type.lower()
            property_type_list = ['wohnung', 'haus']
            if property_type in property_type_list:
                if property_type == 'wohnung':
                    property_type = 'apartment'
                elif property_type == 'haus':
                    property_type = 'house'
                apartment_url = apartment_div.css('div.image a::attr(href)').extract()
                url = 'https://www.baecker-immobilien.de/' + apartment_url[0]
                city = apartment_div.css('span.city::text')[0].extract()
                square_meters = apartment_div.css('div.details.area-details div span.wohnflaeche span span::text').extract()
                rent = apartment_div.css('div.details.area-details div span span::text').extract()
                if len(square_meters) >= 1:
                    rent = rent[-1]
                    square_meters = square_meters[0]
                    yield scrapy.Request(url, callback=self.populate_item, meta={
                        'property_type': property_type,
                        'city': city,
                        'square_meters': square_meters,
                        'rent': rent,
                    })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        rent = response.meta.get('rent')
        rent = rent.split()[0]
        rent = rent.replace(".", "")
        rent = rent.replace(",", ".")
        rent = round(float(rent))
        rent = int(rent)
        # Enforces rent between 0 and 40,000 please dont delete these lines
        if int(rent) <= 0 and int(rent) > 40000:
            return

        square_meters = response.meta.get('square_meters')
        square_meters = square_meters.split()[0]
        square_meters = square_meters.replace(".", "")
        square_meters = square_meters.replace(",", ".")
        square_meters = round(float(square_meters))
        square_meters = int(square_meters)

        property_type = response.meta.get('property_type')

        city = response.meta.get('city')

        title = response.css('div.detail.two-thirds h1::text').extract()

        images = response.css('div.detail.two-thirds div.fotorama div::attr(data-img)').extract()

        apartment_keys = response.css('div.detail.two-thirds div.details div.details-desktop table tr td strong::text').extract()
        apartment_values = response.css('div.detail.two-thirds div.details div.details-desktop table tr td span::text').extract()
        apartment_dict = dict(zip(apartment_keys, apartment_values))

        external_id = apartment_dict['ImmoNr']

        zipcode = apartment_dict['PLZ']

        room_count = apartment_dict['Anzahl Zimmer']
        room_count = room_count.replace(",", ".")
        room_count = round(float(room_count))
        room_count = int(room_count)

        deposit = None
        utilities = None

        if 'Kaution' in apartment_dict.keys():
            deposit = apartment_dict['Kaution']
            deposit = deposit.split()[0]
            deposit = deposit.replace(".", "")
            deposit = deposit.replace(",", ".")
            deposit = round(float(deposit))
            deposit = int(deposit)

        if 'Nebenkosten' in apartment_dict.keys():
            utilities = apartment_dict['Nebenkosten']
            utilities = utilities.split()[0]
            utilities = utilities.replace(".", "")
            utilities = utilities.replace(",", ".")
            utilities = round(float(utilities))
            utilities = int(utilities)

        if not rent:
            if 'Warmmiete' in apartment_dict.keys():
                rent = apartment_dict['Warmmiete']
                rent = rent.split()[0]
                rent = rent.replace(".", "")
                rent = rent.replace(",", ".")
                rent = round(float(rent))
                rent = int(rent)

        div_exist = response.css('div.wrapper.main div.wrapper-center div.content div.detail')
        if len(div_exist) >= 1:
            item_loader = ListingLoader(response=response)

            description = response.css('div.information.details.obj-box p span span::text').extract()
            description = ' '.join(description)
            description = description.replace('..', '')
            description = description.replace('__', '')
            description = description.replace('\n', '')
            description = description.lower()
            balcony = None
            terrace = None
            parking = None
            dishwasher = None
            washing_machine = None
            if 'balkon' in description:
                balcony = True
            if 'terrasse' in description:
                terrace = True
            if 'stellplatz' in description:
                parking = True
            if 'geschirrspüler' in description:
                dishwasher = True
            if 'waschmaschinen-' in description:
                washing_machine = True

            energy_label = response.css('div.information.details.obj-box div#energieausweis input#energyclass::attr(value)').extract()

            address = zipcode + ' ' + city + ', Germany'
            longitude, latitude = extract_location_from_address(address)
            longitude = str(longitude)
            latitude = str(latitude)

            landlord_name = response.css('div.contact div.information p strong::text').extract()
            landlord_number = response.css('div.contact div.information p::text')[0].extract()
            landlord_number = landlord_number.split(':')[1]
            landlord_number = landlord_number.strip()
            landlord_email = response.css('div.contact div.information p a::text').extract()

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
            #item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            #item_loader.add_value("bathroom_count", bathroom_count) # Int

            #item_loader.add_value("available_date", available_date) # String => date_format

            #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            #item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

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
            #item_loader.add_value("heating_cost", heating_cost) # Int

            item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_number) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
