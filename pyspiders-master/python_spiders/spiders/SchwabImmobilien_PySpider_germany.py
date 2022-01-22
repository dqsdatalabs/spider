# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates, extract_location_from_address


class SchwabimmobilienPyspiderGermanySpider(scrapy.Spider):
    name = "SchwabImmobilien"
    start_urls = ['https://www.schwab-immobilien.de/immobilienangebote/?mt=rent&radius=15']
    allowed_domains = ["schwab-immobilien.de"]
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
        pages_url = response.xpath('.//a[contains(@class, "page-numbers")]/text()').extract()
        max_page = int(pages_url[-1])
        url_list = ['https://www.schwab-immobilien.de/immobilienangebote/?mt=rent&radius=15']
        for i in range(max_page-1):
            next_page_num = i+2
            new_url = 'https://www.schwab-immobilien.de/immobilienangebote/page/' + str(next_page_num) + '/?mt=rent&radius=15#immobilien'
            url_list.append(new_url)
        for url in url_list:
            yield scrapy.Request(url, callback=self.parse_pages)

    def parse_pages(self, response):
        apartments_divs = response.xpath('.//div[contains(@id, "immobilien")]//div[contains(@class, "py-1")]')
        for apartment_div in apartments_divs:
            property_type = response.xpath('.//div[contains(@class, "immo-listing__bg")]//ul//li//span[contains(@class, "key")]/text()').extract()
            if "Wohnfläche" in property_type:
                url = apartment_div.xpath('.//a/@href')[0].extract()
                title = apartment_div.xpath('.//div[contains(@class, "immo-listing__title")]//a/text()').extract()
                total_rent = apartment_div.xpath('.//a[contains(@class, "mb-0")]/text()').extract()
                yield scrapy.Request(url, callback=self.populate_item, meta={"title": title, "total_rent": total_rent})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        property_type = response.xpath('//h1/text()')[0].extract()
        property_type = property_type.split()[0]
        property_type = property_type.strip()
        property_type_list = ["Haus", "Wohnung"]
        if property_type in property_type_list:
            if property_type == "Wohnung":
                property_type = "apartment"
            else:
                property_type = "house"
            rooms_info_keys = response.xpath('.//ul[contains(@class, "immo-expose__head--iconFields")]//li//span[contains(@class, "name")]/text()').extract()
            rooms_info_values = response.xpath('.//ul[contains(@class, "immo-expose__head--iconFields")]//li//span[contains(@class, "value")]/text()').extract()
            rooms_info = dict(zip(rooms_info_keys, rooms_info_values))

            if "Wohnfläche\xa0(ca.)" in rooms_info.keys():
                prices_info_div = response.xpath('.//div[contains(@class, "immo-expose__list-price")]//div[contains(@class, "row")]')
                prices_list = prices_info_div.xpath('.//div[contains(@class, "col-md-16")]//ul[contains(@class, "mb-0")]//li')
                prices_list_keys = prices_list.xpath('.//span[contains(@class, "key")]/text()').extract()
                prices_list_values = prices_list.xpath('.//span[contains(@class, "value")]/text()').extract()
                prices = dict(zip(prices_list_keys, prices_list_values))
                utilities = 0
                heating_cost = 0
                deposit = None
                total_rent = response.meta.get("total_rent")
                total_rent = total_rent[0]

                if total_rent != "Preis auf Anfrage":
                    item_loader = ListingLoader(response=response)

                    total_rent = total_rent.replace("€", "")
                    total_rent = total_rent.replace(".", "")
                    total_rent = total_rent.replace(",", ".")
                    total_rent = float(total_rent)
                    total_rent = round(total_rent)
                    total_rent = int(total_rent)

                    if "Nebenkosten" in prices.keys():
                        utilities = prices["Nebenkosten"]
                        if "€" not in utilities:
                            utilities = 0
                        else:
                            utilities = utilities.replace("€", "")
                            utilities = utilities.replace(".", "")
                            utilities = utilities.replace(",", ".")
                            utilities = float(utilities)
                            utilities = round(utilities)
                            utilities = int(utilities)

                    if "Heizkosten" in prices.keys():
                        heating_cost = prices["Heizkosten"]
                        if "€" in heating_cost:
                            heating_cost = heating_cost.replace("€", "")
                            heating_cost = heating_cost.replace(".", "")
                            heating_cost = heating_cost.replace(",", ".")
                            heating_cost = float(heating_cost)
                            heating_cost = round(heating_cost)
                            heating_cost = int(heating_cost)
                        else:
                            heating_cost = 0

                    deposit_div = prices_info_div.xpath('.//div[contains(@class, "col-12")]/text()').extract()
                    if "Kaution" in deposit_div:
                        deposit = deposit_div[1]
                        deposit = deposit.replace("€", "")
                        deposit = deposit.replace(".", "")
                        deposit = deposit.replace(",", ".")
                        deposit = float(deposit)
                        deposit = round(deposit)
                        deposit = int(deposit)

                    rent = total_rent - (utilities + heating_cost)

                    if utilities == 0:
                        utilities = None
                    if heating_cost == 0:
                        heating_cost = None

                    address = prices_info_div.xpath('.//div[contains(@class, "col-md-16")]//p/text()').extract()

                    title = response.xpath('.//h1//small/text()')[0].extract()
                    title = title.replace("\n", "")

                    images = response.xpath('.//div[contains(@class, "lightgallery")]//a/@href').extract()

                    square_meters = rooms_info["Wohnfläche\xa0(ca.)"]
                    square_meters = square_meters[:-2]
                    square_meters = square_meters.replace(".", "")
                    square_meters = square_meters.replace(",", ".")
                    square_meters = float(square_meters)
                    square_meters = round(square_meters)
                    square_meters = int(square_meters)

                    room_count = rooms_info["Zimmer"]
                    room_count = room_count.strip()
                    room_count = room_count.replace(".", "")
                    room_count = room_count.replace(",", ".")
                    room_count = float(room_count)
                    room_count = round(room_count)
                    room_count = int(room_count)

                    property_floor_num = response.css('ul.immo-expose__list-price--list.pb-1 li')
                    floor_id_keys = property_floor_num.css('span.key::text').extract()
                    floor_id_values = property_floor_num.css('span.value::text').extract()
                    floor_id = dict(zip(floor_id_keys, floor_id_values))
                    external_id = None
                    floor = None
                    available_date = None
                    if "Objekt-Nr" in floor_id.keys():
                        external_id = floor_id["Objekt-Nr"]
                    if "Lage im Objekt (Etage)" in floor_id.keys():
                        floor = floor_id["Lage im Objekt (Etage)"]
                    if "verfügbar ab" in floor_id.keys():
                        available_date_exist = floor_id["verfügbar ab"]
                        if "." in available_date_exist:
                            available_date = available_date_exist.split(".")
                            day = available_date[0]
                            month = available_date[1]
                            year = available_date[2]
                            available_date = year.strip() + "-" + month.strip() + "-" + day.strip()

                    tabs_all = response.xpath('.//script/text()').extract()
                    description = []
                    for tab in tabs_all:
                        if "<b-card no-body>" in tab:
                            details_info = tab
                            details_info = details_info.split("<b-tab")
                            for detail in details_info:
                                if "Beschreibung" in detail:
                                    detail = detail.split("<p>")
                                    detail = detail[1:]
                                    for item in detail:
                                        description_first = item.split("</p>")[0]
                                        description.append(description_first)
                    description = " ".join(description)
                    description = description.replace("...", "")
                    description = description.replace("  ", " ")
                    description = description.replace("\n", "")

                    energy_details = response.css('ul.epass__info-list li')
                    energy_label = None
                    for row in energy_details:
                        li_label = row.xpath('./text()').extract()
                        li_label = (li_label[0]).strip()
                        if li_label == "Energieeffizienzklasse":
                            energy_label = row.xpath('.//span/text()').extract()

                    landlord_name = "Schwab Immobilien"
                    landlord_number = "0201 821 555 - 0"

                    address = (address[0].strip()) + ", Germany"
                    longitude, latitude = extract_location_from_address(address)
                    longitude = str(longitude)
                    latitude = str(latitude)
                    zipcode, city, no_address = extract_location_from_coordinates(longitude, latitude)

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
                    #item_loader.add_value("bathroom_count", bathroom_count) # Int

                    item_loader.add_value("available_date", available_date) # String => date_format

                    #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                    #item_loader.add_value("furnished", furnished) # Boolean
                    #item_loader.add_value("parking", parking) # Boolean
                    #item_loader.add_value("elevator", elevator) # Boolean
                    #item_loader.add_value("balcony", balcony) # Boolean
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

                    item_loader.add_value("energy_label", energy_label) # String

                    # # LandLord Details
                    item_loader.add_value("landlord_name", landlord_name) # String
                    item_loader.add_value("landlord_phone", landlord_number) # String
                    #item_loader.add_value("landlord_email", landlord_email) # String

                    self.position += 1
                    yield item_loader.load_item()
