# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address


class WiesnerimmobilienPyspiderGermanySpider(scrapy.Spider):
    name = "WiesnerImmobilien"
    start_urls = ['https://www.frankfurt-oder-immobilien.de/index.php/alle-angebote/vermietungsangebote']
    allowed_domains = ["frankfurt-oder-immobilien.de"]
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
        apartments_divs = response.css('div.default-view-tile div.row div.col-lg-4')
        for apartment_div in apartments_divs:
            apartment_all = apartment_div.css('div.item-values')
            type_city = apartment_all.css('p.item-labels span span::text').extract()
            property_type = type_city[0]
            if property_type == "Wohnung":
                address = type_city[1] + ", " + type_city[2]
                address = address.split("(")[0]
                address = address.strip()
                title = apartment_all.css('p.item-text span::text')[0].extract()
                if "RESERVIERT" not in title:
                    apartment_data = apartment_all.css('div.item-fields p')
                    rent_exist = apartment_data.css('span.field-preis::text')[0].extract()
                    if rent_exist != "auf Anfrage":
                        rent = rent_exist.split()[0]
                        rent = int(rent)
                        external_id = apartment_data.css('span.field-anbieter_objekt_nr::text')[0].extract()
                        square_meters = apartment_data.css('span.field-flaeche::text')[0].extract()
                        square_meters = square_meters.split()[0]
                        square_meters = int(square_meters)
                        room_count = apartment_data.css('span.field-anzahl_zimmer::text')[0].extract()
                        room_count = room_count.replace(",", ".")
                        room_count = float(room_count)
                        room_count = round(room_count)
                        room_count = int(room_count)
                        apartment_url = apartment_div.css('div.item-image a::attr(href)')[0].extract()
                        url = "https://www.frankfurt-oder-immobilien.de" + apartment_url
                        apartment_dict = {
                            'address': address,
                            'title': title,
                            'rent': rent,
                            'external_id': external_id,
                            'square_meters': square_meters,
                            'room_count': room_count,
                        }
                        yield scrapy.Request(url, callback=self.populate_item, meta=apartment_dict)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.meta.get('title')
        address = response.meta.get('address')
        rent = response.meta.get('rent')
        external_id = response.meta.get('external_id')
        square_meters = response.meta.get('square_meters')
        room_count = response.meta.get('room_count')
        property_type = "apartment"

        landlord_name = "WIESNER REAL ESTATE Mrs. Jacqueline Röstel"
        landlord_email = "vermobilien@frankfurt-oder-immobilien.de"
        landlord_number = "0335 55874-0"

        apartment_info = response.css('div.tab-content')
        street = apartment_info.css('span.field-strasse::text')[0].extract()
        street_number = apartment_info.css('span.field-hausnr::text')[0].extract()
        address = street + " " + street_number + ", " + address

        apartment_table = apartment_info.css('div.row-imo-fieldlist div div table tbody tr')
        floor = apartment_table.css('td span.field-etage::text').extract()

        zipcode = apartment_table.css('td span.field-plz::text').extract()

        utilities = apartment_table.css('td span.field-nebenkosten::text')[0].extract()
        utilities = utilities.split()[0]
        utilities = utilities.replace(".", "")
        utilities = utilities.replace(",", ".")
        utilities = round(float(utilities))
        utilities = int(utilities)

        heating_cost = apartment_table.css('td span.field-heizkosten::text')[0].extract()
        heating_cost = heating_cost.split()[0]
        heating_cost = heating_cost.replace(".", "")
        heating_cost = heating_cost.replace(",", ".")
        heating_cost = round(float(heating_cost))
        heating_cost = int(heating_cost)

        deposit = apartment_table.css('td span.field-kaution::text')[0].extract()
        deposit = deposit.split()[0]
        deposit = deposit.replace(".", "")
        deposit = deposit.replace(",", ".")
        deposit = round(float(deposit))
        deposit = int(deposit)

        parking = apartment_table.css('td span.field-anzahl_stellplaetze::text').extract()
        if len(parking) >= 1:
            parking = True
        else:
            parking = None

        bathroom_count = apartment_table.css('td span.field-anzahl_badezimmer::text')[0].extract()
        bathroom_count = bathroom_count.replace(",", ".")
        bathroom_count = float(bathroom_count)
        bathroom_count = round(bathroom_count)
        bathroom_count = int(bathroom_count)

        energy_label = apartment_table.css('td span.field-energiepass_gueltig_bis::text')[0].extract()
        energy_label = energy_label.split("/")[1]
        energy_label = energy_label.strip()

        available_date = apartment_table.css('td span.field-verfuegbar_ab_freitext::text')[0].extract()
        if "sofort" in available_date:
            available_date = None
        else:
            available_date = available_date.split()
            year = available_date[2].strip()
            month = available_date[1].strip()
            if "Februar" in month:
                month = "02"
            day = available_date[0].strip()
            day = day.replace(".", "")
            available_date = year + "-" + month + "-" + day

        description = response.css('span.field-texte_beschreibung::text').extract()
        description = " ".join(description)
        description = description.lower()
        description = description.replace("\n", "")
        balcony = None
        if "balkon" in description:
            balcony = True
        if "Pkw-Stellplätze" in description:
            parking = True
        if "pkw-stellplatz" in description:
            parking = True

        address = address + ", Germany"
        city = apartment_table.css('td span.field-regionaler_zusatz::text')[0].extract()
        longitude, latitude = extract_location_from_address(address)
        longitude = str(longitude)
        latitude = str(latitude)

        images_all_values = response.css('div.default-incl-slideshow div div div div div.item a::attr(href)').extract()
        images_all_keys = response.css('div.default-incl-slideshow div div div div div.item a::attr(title)').extract()
        images_all = dict(zip(images_all_keys, images_all_values))
        images = []
        floor_plan_images = None
        for image in images_all.values():
            image = "https://www.frankfurt-oder-immobilien.de" + image
            images.append(image)
        if "Grundriss" in images_all.keys():
            floor_plan_images = images_all["Grundriss"]
            floor_plan_images = "https://www.frankfurt-oder-immobilien.de" + floor_plan_images
        if floor_plan_images:
            location = images.index(floor_plan_images)
            del images[location]

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
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
