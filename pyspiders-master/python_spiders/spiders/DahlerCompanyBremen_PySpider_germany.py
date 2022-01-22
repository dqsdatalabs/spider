# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates
import json
import requests


class DahlercompanybremenPyspiderGermanySpider(scrapy.Spider):
    name = "DahlerCompanyBremen"
    start_urls = ['https://solr8.dr-heiko-hofer.de/solr/dahler/select']
    allowed_domains = ["dahlercompany.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            body = {"query":"*","filter":["ss_language:(\"de\")","ss_type_of_marketing:(MIETE_PACHT)"],"facet":{"office_name":{"type":"terms","field":"ss_office_name","domain":{"excludeTags":"office_name"},"mincount":0,"sort":"index","limit":100},"region":{"type":"terms","field":"ss_region","domain":{"excludeTags":"region,locality"},"mincount":0,"limit":100},"office_tid_region":{"type":"terms","field":"ss_office_tid_region","domain":{"excludeTags":"region,locality,office"},"mincount":0,"limit":100},"locality":{"type":"terms","field":"ss_locality","domain":{"excludeTags":"locality"},"sort":"index","limit":1000},"property_type_name":{"type":"terms","field":"ss_property_type_name","mincount":1,"limit":100},"marketing_type_name":{"type":"terms","field":"ss_type_of_marketing","mincount":0,"limit":100},"min_price":{"type":"func","func":"min(fts_price)"},"max_price":{"type":"func","func":"max(fts_price)"},"min_chambers":{"type":"func","func":"min(fts_chamber_count)"},"max_chambers":{"type":"func","func":"max(fts_chamber_count)"},"min_living_area":{"type":"func","func":"min(fts_living_area)"},"max_living_area":{"type":"func","func":"max(fts_living_area)"},"min_plot_area":{"type":"func","func":"min(fts_plot_area)"},"max_plot_area":{"type":"func","func":"max(fts_plot_area)"}},"sort":"fts_price desc","limit":500}
            headers = {"user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"}
            r = requests.post(url,json=body,headers=headers)
            apartments_json = json.loads(r.text)
            apartments_json = apartments_json["response"]["docs"]
            for apartment_dict in apartments_json:
                property_type = apartment_dict["ss_property_type_machine_name"]
                property_type = property_type.lower()
                property_type_options = ["haus", "wohnung"]
                if property_type in property_type_options:
                    rent = apartment_dict["fts_price"]
                    if rent != 0.0:
                        long_lat = apartment_dict["locs_geo"]
                        city = apartment_dict["zs_location_text"]
                        link_id = apartment_dict["ss_import_id"]
                        apartment_url = "https://www.dahlercompany.com/de/node/" + link_id
                        images = apartment_dict["sm_image_style_uri_list"]
                        title = apartment_dict["ss_title"]
                        square_meters = None
                        room_count = None
                        if "fts_living_area" in apartment_dict.keys():
                            square_meters = apartment_dict["fts_living_area"]
                        if "fts_chamber_count" in apartment_dict.keys():
                            room_count = apartment_dict["fts_chamber_count"]
                        external_id = apartment_dict["ss_property_id"]
                        apartment_info = {
                            "long_lat": long_lat,
                            "city": city,
                            "images": images,
                            "title": title,
                            "square_meters": square_meters,
                            "room_count": room_count,
                            "rent": rent,
                            "property_type": property_type,
                            "external_id": external_id,
                        }
                        yield scrapy.Request(apartment_url, callback=self.populate_item, meta=apartment_info)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rent = response.meta.get("rent")
        rent = float(rent)
        rent = round(rent)
        rent = int(rent)

        long_lat = response.meta.get("long_lat")
        long_lat = long_lat.split(",")
        latitude = long_lat[0]
        longitude = long_lat[1]
        zipcode, no_city, address = extract_location_from_coordinates(longitude, latitude)

        city = response.meta.get("city")
        title = response.meta.get("title")
        external_id = response.meta.get("external_id")

        square_meters = None
        if response.meta.get("square_meters"):
            square_meters = response.meta.get("square_meters")
            square_meters = float(square_meters)
            square_meters = round(square_meters)
            square_meters = int(square_meters)

        room_count = None
        if response.meta.get("room_count"):
            room_count = response.meta.get("room_count")
            room_count = float(room_count)
            room_count = round(room_count)
            room_count = int(room_count)

        if not room_count:
            room_count = 1

        property_type = response.meta.get("property_type")
        if property_type == "haus":
            property_type = "house"
        elif property_type == "wohnung":
            property_type = "apartment"

        apartment_info = response.css('div#immo-all-data table tr td::text').extract()
        apartment_info_keys = []
        apartment_info_values = []
        for i in range(0, len(apartment_info)):
            if i % 2:
                apartment_info_values.append(apartment_info[i])
            else:
                apartment_info_keys.append(apartment_info[i])
        apartment_info = dict(zip(apartment_info_keys, apartment_info_values))
        utilities = None
        deposit = None
        heating_cost = None
        parking = None
        energy_label = None
        available_date = None
        if "Nebenkosten" in apartment_info.keys():
            utilities = apartment_info ["Nebenkosten"]
            utilities = utilities.split("–")[0]
            utilities = utilities.split(" ")[1]
            utilities = utilities.replace(".", "")
            utilities = utilities.replace(",", ".")
            utilities = float(utilities)
            utilities = round(utilities)
            utilities = int(utilities)

        if "Kaution" in apartment_info.keys():
            deposit = apartment_info["Kaution"]
            if "€" in deposit:
                deposit = deposit.split("–")[0]
                deposit = deposit.split(" ")[1]
                deposit = deposit.replace(".", "")
                deposit = deposit.replace(",", ".")
                deposit = float(deposit)
                deposit = round(deposit)
                deposit = int(deposit)
            else:
                deposit = deposit.split("–")[0]
                deposit = deposit.split(" ")[0]
                if deposit.isdigit():
                    deposit = int(deposit)
                    deposit = deposit * rent
                elif deposit == "drei":
                    deposit = 3 * rent

        if "Heizkosten" in apartment_info.keys():
            heating_cost = apartment_info["Heizkosten"]
            heating_cost = heating_cost.split("–")[0]
            heating_cost = heating_cost.split(" ")[1]
            heating_cost = heating_cost.replace(".", "")
            heating_cost = heating_cost.replace(",", ".")
            heating_cost = float(heating_cost)
            heating_cost = round(heating_cost)
            heating_cost = int(heating_cost)

        if "Anzahl der Garagenstellplätze" in apartment_info.keys():
            parking = True

        if "Klasse" in apartment_info.keys():
            energy_label = apartment_info["Klasse"]
            if "keine Angaben" in energy_label:
                energy_label = None

        if "Übergabe" in apartment_info.keys():
            available_date = apartment_info["Übergabe"]
            if "sofort" in available_date:
                available_date = None
            elif "nach Absprache" in available_date:
                available_date = None
            elif "nach Vereinbarung" in available_date:
                available_date = None
            else:
                if "." in available_date:
                    available_date = available_date.split(".")
                    day = available_date[0]
                    month = available_date[1]
                    year = available_date[2]
                    available_date = year.strip() + "-" + month.strip() + "-" + day.strip()
                else:
                    available_date = available_date.split()
                    year = available_date[-1]
                    month = available_date[-2]
                    if month == "November":
                        month = "11"
                    elif month == "51":
                        month = "12"
                    day = "01"
                    available_date = year.strip() + "-" + month.strip() + "-" + day.strip()

        description_one = response.css('div.field-objektbeschreibung ul li::text').extract()
        description_one = " ".join(description_one)
        if not description_one:
            description_one = response.css('div.field-objektbeschreibung p::text')[0].extract()
        description_two = response.css('div.field-lage.field-story ul li::text').extract()
        description_two = " ".join(description_two)
        if not description_two:
            description_two = response.css('div.field-lage.field-story p::text')[0].extract()
        description = description_two + description_one

        furnishing = response.css('div.field-ausstatt-beschr ul li::text').extract()
        terrace = None
        balcony = None
        for item in furnishing:
            if "terrasse" in item:
                terrace = True
            if "Balkon" in item:
                balcony = True
            if "Stellplatzflächen" in item:
                parking = True

        images_all = response.css('div.immo-gallery-slide-wrapper div img::attr(src)').extract()
        images = []
        for image in images_all:
            if ".gif" not in image:
                images.append(image)

        landlord_info = response.css('div#office-imprint div.container div.col-imprint')
        landlord_name = landlord_info.xpath('.//h3/text()').extract()
        landlord_name = "DAHLER & COMPANY " + landlord_name[0]
        landlord_number = landlord_info.xpath('.//div//a[contains(@title, "Telefonnummer")]/text()').extract()
        landlord_email = landlord_info.xpath('.//div//a[contains(@itemprop, "email")]/text()').extract()

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

        item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
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
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
