# -*- coding: utf-8 -*-
# Author: Abdulrahman Abbas


import scrapy

from ..helper import extract_number_only, convert_to_numeric, extract_date, string_found, \
    extract_location_from_coordinates, extract_location_from_address, remove_white_spaces
from ..loaders import ListingLoader


class FrankhoffmannImmobilienDeSpider(scrapy.Spider):
    name = "frankhoffmann_immobilien_de"
    start_urls = [
        'https://www.frankhoffmann-immobilien.de/immobilien/?post_type=immomakler_object&paged=1&vermarktungsart=miete&nutzungsart=wohnen&typ=wohnung&ort=&center=&radius=25&objekt-id=&collapse=&von-qm=0.00&bis-qm=805.00&von-zimmer=0.00&bis-zimmer=25.00&von-kaltmiete=0.00&bis-kaltmiete=6900.00&von-kaufpreis=0.00&bis-kaufpreis=12025000.00',
        'https://www.frankhoffmann-immobilien.de/immobilien/?post_type=immomakler_object&paged=1&vermarktungsart=miete&nutzungsart=wohnen&typ=haus&ort=&center=&radius=25&objekt-id=&collapse=&von-qm=0.00&bis-qm=805.00&von-zimmer=0.00&bis-zimmer=25.00&von-kaltmiete=0.00&bis-kaltmiete=6900.00&von-kaufpreis=0.00&bis-kaufpreis=12025000.00'
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
        apartment_page_links = response.xpath('//h3[@class="property-title"]//a')
        yield from response.follow_all(apartment_page_links, self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        title = response.xpath('//h1[@class="property-title"]//text()').get()
        property_details = response.xpath('//div[@class="property-details panel panel-default"]//text()').getall()
        details1 = response.xpath('//div[@class="property-features panel panel-default"]//text()').getall()
        details2 = " ".join(response.xpath('//div[@class="property-description panel panel-default"]//text()').getall())
        details3 = response.xpath(
            '//div[@class="property-epass panel panel-default"]//li[last()]/div//div[last()]//text()').get()
        contact_info = response.xpath('//div[@class="property-contact panel panel-default"]//text()').getall()

        amenities = " ".join(details1) + details2
        description = remove_white_spaces(details2.split("Ausstattung")[0]).replace("Objekt\xadbeschreibung", "")
        if "ACHTUNG:" in description:
            description = description.split("datenschutz.")[-1].replace("-", '')

        external_id = None
        if "Objekt ID" in property_details:
            position = property_details.index("Objekt ID")
            external_id = (property_details[position + 2])

        rent = None
        if "Kaltmiete" in property_details:
            position = property_details.index("Kaltmiete")
            rent = int(convert_to_numeric(extract_number_only(property_details[position + 2])))

        heating_cost = None
        if "Heizkosten" in property_details:
            position = property_details.index("Heizkosten")
            heating_cost = convert_to_numeric(extract_number_only(property_details[position + 2]))

        deposit = None
        if "Kaution" in property_details:
            position = property_details.index("Kaution")
            deposit = property_details[position + 2]

        utilities = None
        if "Nebenkosten" in property_details:
            position = property_details.index("Nebenkosten")
            utilities = property_details[position + 2]

        property_type = 'apartment'
        if "Objekttypen" in property_details and "Haus" in property_details:
            property_type = 'house'

        square_meters = None
        if "Wohnfläche\xa0ca." in property_details:
            position = property_details.index("Wohnfläche\xa0ca.")
            square_meters = int(convert_to_numeric(extract_number_only(property_details[position + 2])))

        location = None
        if "Adresse" in property_details:
            position = property_details.index("Adresse")
            location = property_details[position + 2] + property_details[position + 3]

        longitude, latitude = extract_location_from_address(location)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        room_count = None
        if "Zimmer" in property_details:
            position = property_details.index("Zimmer")
            room_count = convert_to_numeric(extract_number_only(property_details[position + 2]))
            x = isinstance(room_count, float)
            if x:
                room_count += 0.5

        available_date = None
        if "Verfügbar ab" in property_details:
            position = property_details.index("Verfügbar ab")
            available_date = extract_date(property_details[position + 2])

        energy_label = None
        if details3:
            energy_label = details3

        bathroom_count = None
        if string_found(['WC', 'Dusche', "Badewanne", 'Bad', 'Wohlfühlbad', 'Vollbad', 'Badezimmer', 'Gäste-WC'],
                        amenities):
            if "Gäste-WC" in amenities:
                bathroom_count = 2
            else:
                bathroom_count = 1

        elevator = False
        if string_found(['Aufzug', 'Aufzügen', 'Fahrstuhl', 'Personenaufzug', 'Personenfahrstuhl'], amenities):
            elevator = True

        parking = False
        if string_found(
                ['Tiefgarage', 'Stellplatz', 'Garage', 'Tiefgaragenstellplatz', 'Außenstellplatz', 'Stellplätze',
                 'Einzelgarage'], amenities):
            parking = True

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

        images = response.xpath('//div//img//@data-big').getall()
        for image in images:
            if "logo" in image:
                images.remove(image)
            if "benutzerBild" in image:
                images.remove(image)

        landlord_number = None
        if "Tel. Zentrale" in contact_info:
            position = contact_info.index("Tel. Zentrale")
            landlord_number = contact_info[position + 2]

        landlord_name = None
        if "Name" in contact_info:
            position = contact_info.index("Name")
            landlord_name = contact_info[position + 2]

        landlord_email = None
        if "E-Mail Zentrale" in contact_info:
            position = contact_info.index("E-Mail Zentrale")
            landlord_email = contact_info[position + 2]

        # # MetaData
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String

        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", location)  # String
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type)  # String
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", available_date)  # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished)  # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        item_loader.add_value("deposit", deposit)  # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities)  # Int
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
