# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
from re import T
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address


class Amarc21PyspiderGermanySpider(scrapy.Spider):
    name = "Amarc21"
    start_urls = ['https://www.amarc21.de/aktuelle-angebote/?zip=&ort=&country=&radius=&vermarktungsart=miete&objektnr_extern=&street=&land=']
    allowed_domains = ["amarc21.de"]
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
        pages_number = response.css('p.post-nav-links a.page-numbers::text')[-1].extract()
        pages_number = int(pages_number)
        urls = ['https://www.amarc21.de/aktuelle-angebote/?zip=&ort=&country=&radius=&vermarktungsart=miete&objektnr_extern=&street=&land=']
        for i in range(pages_number - 1):
            page_url = 'https://www.amarc21.de/aktuelle-angebote/page/' + str(i + 2) + '/?zip&ort&country&radius&vermarktungsart=miete&objektnr_extern&street&land'
            urls.append(page_url)
        for url in urls:
            yield scrapy.Request(url, callback=self.parse_pages, dont_filter=True)

    def parse_pages(self, response):
        apartments_divs = response.css('div.col-md-3.listobject div.inner')
        for apartment_div in apartments_divs:
            url = apartment_div.css('a::attr(href)')[0].extract()
            title = apartment_div.css('div.list-info span.obj-headline::text')[0].extract()
            title = title.strip()
            title = ' '.join(title.split())
            apartment_table = apartment_div.css('div.list-info table tr')
            apartment_table_keys = apartment_table.xpath('.//td[1]/text()').extract()
            apartment_table_values = apartment_table.xpath('.//td[2]/text()').extract()
            apartment_dict = dict(zip(apartment_table_keys, apartment_table_values))
            external_id = apartment_dict['Objektnummer']

            property_type = None
            if 'Objekttyp' in apartment_dict.keys():
                property_type = apartment_dict['Objekttyp']
                property_type_refuse = ['Verkaufsfläche', 'Gastronomie', 'Bürofläche', 'Bürohaus', 'Lagerhalle',
                                        'Speditionslager', 'Büroetage', 'Hochparterre', 'Wohn- und Geschäftshaus']
                if property_type in property_type_refuse:
                    property_type = None

            zipcode = apartment_dict['PLZ']
            city = apartment_dict['Ort']
            if 'Bundesland' in apartment_dict.keys():
                state = apartment_dict['Bundesland']
                city = city + ', ' + state

            square_meters = None
            if 'Wohnfläche' in apartment_dict.keys():
                square_meters = apartment_dict['Wohnfläche']

            rent = None
            if 'Kaltmiete' in apartment_dict.keys():
                rent = apartment_dict['Kaltmiete']

            if property_type and rent:
                yield scrapy.Request(url, callback=self.populate_item, meta={
                    'external_id': external_id,
                    'city': city,
                    'square_meters': square_meters,
                    'title': title,
                    'rent': rent,
                    'zipcode': zipcode,
                    'property_type': property_type,
                })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        title = response.meta.get('title')
        

        external_id = response.meta.get('external_id')

        city = response.meta.get('city')
        zipcode = response.meta.get('zipcode')
        address = zipcode + ' ' + city + ', Germany'
        longitude, latitude = extract_location_from_address(address)
        longitude = str(longitude)
        latitude = str(latitude)

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
        if square_meters:
            square_meters = square_meters.split()[0]
            square_meters = square_meters.replace(".", "")
            square_meters = square_meters.replace(",", ".")
            square_meters = round(float(square_meters))
            square_meters = int(square_meters)
        else:
            square_meters = None

        apartment_table = response.css('div.col-md-7.details-table tr')
        apartment_table_keys = apartment_table.xpath('.//td[1]/text()').extract()
        apartment_table_values = apartment_table.xpath('.//td[2]/text()').extract()
        apartment_dict = dict(zip(apartment_table_keys, apartment_table_values))

        room_count = None
        if 'Anzahl Zimmer' in apartment_dict.keys():
            room_count = apartment_dict['Anzahl Zimmer']
            room_count = room_count.replace(",", ".")
            room_count = round(float(room_count))
            room_count = int(room_count)
        if not room_count or room_count==0:
            room_count=1

        property_type_title = apartment_dict['Objektart']
        if 'Lager' not in property_type_title and 'Laden' not in property_type_title:
            property_type = property_type_title
            if 'haus' in property_type.lower():
                property_type = 'house'
            else:
                property_type = 'apartment'

            landlord_name = response.css('div.col-md-3.details-asp div.container div.row div.asp-info strong.asp-name::text').extract()
            landlord_email = response.css('div.col-md-3.details-asp div.container div.row div.asp-info ul li a::text').extract()
            landlord_number = response.css('div.col-md-3.details-asp div.container div.row div.asp-info ul li::text')[0].extract()
            landlord_number = landlord_number.replace('\n', '')
            landlord_number = landlord_number.replace('\t', '')
            landlord_number = (landlord_number.split(':')[1]).strip()

            description = response.css('div#beschreibung::text').extract()

            floor_plan_images = response.css('div#grundriss div a::attr(href)').extract()
            if len(floor_plan_images) < 1:
                floor_plan_images = None

            furnishing = response.css('div#ausstattung::text').extract()
            furnishing = ' '.join(furnishing)
            furnishing = ' '.join(furnishing.split())
            furnishing = furnishing.lower()
            elevator = None
            terrace = None
            balcony = None
            bathroom_count = None
            parking = None
            washing_machine = None
            pets_allowed = None
            if 'aufzug' in furnishing:
                elevator = True
            if 'terrasse' in furnishing:
                terrace = True
            if 'balcon' in furnishing:
                balcony = True
            if 'stellplätze' in furnishing:
                parking = True
            if 'waschmaschine' in furnishing:
                washing_machine = True
            if 'haustier' in furnishing:
                pets_allowed = True
            if 'bad' in furnishing:
                bathroom_count = 1

            energy = response.css('div#energieausweis p::text').extract()
            energy_label = None
            for item in energy:
                if 'Energieeffizienzklasse' in item:
                    energy_label = item.split(':')[1]
                    energy_label = energy_label.replace('\n', '')
                    energy_label = energy_label.replace('\t', '')

            images = response.css('div.main-gallery a::attr(href)').extract()

            item_loader = ListingLoader(response=response)
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
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            #item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            #item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            #item_loader.add_value("deposit", deposit) # Int
            #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            #item_loader.add_value("utilities", utilities) # Int
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
