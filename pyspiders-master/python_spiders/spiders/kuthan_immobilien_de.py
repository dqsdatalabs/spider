# -*- coding: utf-8 -*-
# Author: Abdulrahman Abbas
import scrapy

from ..helper import extract_location_from_address, extract_location_from_coordinates, convert_to_numeric, \
    extract_number_only, remove_unicode_char, string_found, remove_white_spaces
from ..loaders import ListingLoader


class KuthanImmobilienDeSpider(scrapy.Spider):
    name = "kuthan_immobilien_de"
    start_urls = ['https://www.kuthan-immobilien.de/angebote-gesuche/immobilienangebote/']
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        for item in response.xpath('//div[@class="card__inner"]'):
            url = item.xpath('.//a[@class="media"]/@href').get()
            if "+++" not in item.xpath('.//h2/text()').get():
                yield scrapy.Request("https://www.kuthan-immobilien.de/{}".format(url), callback=self.populate_item)

        next_page = response.xpath('//a[@class="arrow arrow--next"]/@href').get()
        if next_page is not None:
            yield scrapy.Request("https://www.kuthan-immobilien.de/{}".format(next_page), callback=self.parse)

    # 3. SCRAPING level 3

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # START
        data = response.xpath('//*[@id="wrap"]/section/section[4]/div/div/div[2]/div[1]/div/ul//text()').getall()

        if "Kaufpreis:" not in data:
            title = remove_unicode_char(response.xpath('//*[@id="wrap"]/section/section[2]/div/h1/text()').get())
            description = remove_unicode_char(" ".join(
                response.xpath('//*[@id="wrap"]/section/section[4]/div/div/div[1]/div[1]/div/p/text()')[:-1].getall()))
            all_details = remove_unicode_char(" ".join(
                response.xpath('//*[@id="wrap"]/section/section[4]/div/div/div[1]/div[1]/div/p/text()').getall()))
            furnishing = " ".join(
                response.xpath('//*[@id="wrap"]/section/section[4]/div/div/div[1]/div[2]/div/p/text()').getall())
            amenities = all_details + furnishing

            energy_label = response.xpath(
                '//*[@id="wrap"]/section/section[4]/div/div/div[2]/div[2]/div[1]/ul/li[4]/span[2]//text()').get()

            external_id = None
            if "Objekt-ID:" in data:
                position = data.index("Objekt-ID:")
                external_id = data[position + 1]

            location = None
            if "Lage:" in data:
                position = data.index("Lage:")
                location = remove_unicode_char(data[position + 1])
            longitude, latitude = extract_location_from_address(location)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

            property_type = None
            if "Immobilienart:" in data:
                position = data.index("Immobilienart:")
                pro_type = data[position + 1]
                if pro_type == "Reihenmittelhaus":
                    property_type = "house"
                else:
                    property_type = "apartment"

            room_count = None
            if "Zimmer:" in data:
                position = data.index('Zimmer:')
                room_count = convert_to_numeric(data[position + 1])
                if type(room_count) is float:
                    room_count += 0.5

            floor = None
            if "Etage:" in data:
                position = data.index("Etage:")
                floor = data[position + 1]

            square_meters = None
            if "Wohnfläche:" in data:
                position = data.index("Wohnfläche:")
                square_meters = int(convert_to_numeric(extract_number_only(data[position + 1])))

            rent = None
            if "Nettokaltmiete:" in data:
                position = data.index("Nettokaltmiete:")
                rent = convert_to_numeric(extract_number_only(data[position + 1]))

            utilities = None
            if "Nebenkosten:" in data:
                position = data.index("Nebenkosten:")
                utilities = data[position + 1]

            heating_cost = None
            if "Warmmiete:" in data:
                position = data.index("Warmmiete:")
                cost = convert_to_numeric(extract_number_only(data[position + 1]))
                heating_cost = cost - rent

            deposit = None
            if "Kaution:" in data:
                position = data.index("Kaution:")
                deposit = data[position + 1]

            parking = False
            if string_found(
                    ['Tiefgaragenstellplatz', 'Garage', 'Außenstellplatz', 'Tiefgarage', 'Stellplatz', 'Stellplätze',
                     'Einzelgarage'], amenities):
                parking = True

            elevator = False
            if string_found(['Aufzug', 'Personenfahrstuhl', 'Personenaufzug'], amenities):
                elevator = True

            balcony = False
            if string_found(['Balkon', 'Balkone', 'Südbalkon'], amenities):
                balcony = True

            terrace = False
            if string_found(['Terrassenwohnung', 'Terrasse', 'Terrasse (ca.)', 'Dachterrasse'], amenities):
                terrace = True

            furnished = False
            if string_found(['vollmöblierten '], amenities):
                furnished = True

            pets_allowed = None
            if "Haustiere:" in data:
                pets_allowed = True

            # END

            # # MetaData
            item_loader.add_value("external_link", response.url)  # String
            item_loader.add_value("external_source", self.external_source)  # String

            item_loader.add_value("external_id", remove_white_spaces(external_id))  # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title)  # String
            item_loader.add_value("description", description)  # String

            # # Property Details
            item_loader.add_value("city", remove_unicode_char(city))  # String
            item_loader.add_value("zipcode", zipcode)  # String
            item_loader.add_value("address", location)  # String
            item_loader.add_value("latitude", str(latitude))  # String
            item_loader.add_value("longitude", str(longitude))  # String
            item_loader.add_value("floor", floor)  # String
            item_loader.add_value("property_type", property_type)  # String
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", 1)  # Int

            # item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
            item_loader.add_value("furnished", furnished)  # Boolean
            item_loader.add_value("parking", parking)  # Boolean
            item_loader.add_value("elevator", elevator)  # Boolean
            item_loader.add_value("balcony", balcony)  # Boolean
            item_loader.add_value("terrace", terrace)  # Boolean
            # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            # item_loader.add_value("washing_machine", washing_machine) # Boolean
            # item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            images = response.xpath('//div[@class="image__wrapper--5-3"]/img/@src').getall()

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
            landlord_email = "".join(
                response.xpath('//div[@class="team--contact"]/span[@class="contact--mail"]/a/text()').getall())
            item_loader.add_xpath("landlord_name", '//div[@class="team--contact"]/strong//text()')  # String
            item_loader.add_xpath("landlord_phone",
                                  '//div[@class="team--contact"]/span[@class="contact--phone"]/a//text()')  # String
            item_loader.add_value("landlord_email", landlord_email)  # String

            self.position += 1
            yield item_loader.load_item()
