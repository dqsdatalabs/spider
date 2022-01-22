# -*- coding: utf-8 -*-
# Author: A.Abbas

import js2xml
import lxml.etree
import scrapy
from parsel import Selector

from ..helper import *
from ..loaders import ListingLoader


class RenateWeberImmobilienDeSpider(scrapy.Spider):
    name = "renate_weber_immobilien_de"
    start_urls = ['https://www.renate-weber-immobilien.de/knowfuncMids.js?t=00001642151657']
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
        javascript = response.xpath('//text()').get()
        xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
        selector = Selector(text=xml)
        for url in selector.xpath('//array//object'):
            urls = url.xpath('.//property[@name="link"]//text()').get().replace(
                'www.renate-weber-immobilien.de/dbmakler.php4https://', '')
            price = convert_to_numeric(url.xpath('.//property[@name="preis"]//text()').get())
            lng = url.xpath('.//property[@name="geol"]//text()').get()
            lat = url.xpath('.//property[@name="geob"]//text()').get()
            zipcode = url.xpath('.//property[@name="plz"]//text()').get()
            city = url.xpath('.//property[@name="ortname"]//text()').get()
            if price < 10000:
                yield scrapy.Request(url=urls, callback=self.populate_item, meta=
                {
                    "lng": lng,
                    "lat": lat,
                    "zipcode": zipcode,
                    "city": city,
                })

    # 3. SCRAPING level 3
    def populate_item(self, response):

        # # START
        images = response.xpath('//img[@class="focusview"]/@src').getall()
        floor_plan_images = response.xpath('//img[@alt="Grundriss Wohnung Mötzingen"]/@src').get()
        title = response.xpath('//div[@class="col-pt-12"]/h1//text()').get()
        address = response.xpath('//div[@class="pd"]//div[2]//text()').get()
        external_id = response.xpath('//div[@class="row"]//div[1]//span[@class="wert"]//text()').get()
        square_meters = response.xpath('//div[@class="row"]//div[2]//span[@class="wert"]//text()').get()
        sqm = int(convert_to_numeric(extract_number_only(square_meters)))

        rent = response.xpath('//div[@class="row"]//div[4]//span[@class="wert"]//text()').get()
        description = response.xpath('//div[@class="tab-content"]/div[1]//text()').getall()
        type = response.xpath('//div[@class="pd"]/div[1]//text()').get()

        if type.lower() == "maisonettewohnung":
            property_type = "house"
        else:
            property_type = "apartment"

        details = response.xpath('//div[@class="row weiteredaten"]//div[@class="row"]//span//text()').getall()
        details2 = response.xpath('//table[@class="energietabelle"]//text()').getall()
        all_details = response.xpath('//div[@class="tab-content"]//text()').getall()

        amenities = " ".join(details) + " ".join(all_details)
        utilities = None
        if "Nebenkosten (ca.)" in details:
            position = details.index("Nebenkosten (ca.)")
            utilities = details[position + 1]

        heating_cost = None
        if "Warmmiete (ca.)" in details:
            position = details.index("Warmmiete (ca.)")
            heating_cost = details[position + 1]

        heating_cost = int(extract_number_only(heating_cost)) - (
                int(extract_number_only(rent)) + int(extract_number_only(utilities)))

        energy_label = None
        if "Energieausweis Werteklasse" in details2:
            position = details2.index("Energieausweis Werteklasse")
            energy_label = details2[position + 1]

        deposit = None
        if "Kaution" in details:
            position = details.index("Kaution")
            deposit = details[position + 1]


        bathroom_count = None
        if "Badezimmer" in details:
            position = details.index("Badezimmer")
            bathroom_count = details[position + 1]

        available_date = None
        if "verfügbar ab" in details:
            position = details.index("verfügbar ab")
            available_date = details[position + 1]

        room_count = response.xpath('//div[@class="row"]//div[3]//span[@class="wert"]//text()').get()

        parking = False
        if string_found(
                ['Garage', 'Außenstellplatz', 'Tiefgarage', 'Tiefgaragenstellplatz', 'Stellplatz', 'Stellplätze',
                 'Einzelgarage'], amenities):
            parking = True

        elevator = False
        if string_found(['Aufzug'], amenities):
            elevator = True

        balcony = False
        if string_found(['Balkon', 'Balkone', 'Südbalkon'], amenities):
            balcony = True

        terrace = False
        if string_found(['Terrassenwohnung', 'Terrasse', 'Terrasse (ca.)'], amenities):
            terrace = True

        washing_machine = False
        if string_found(['gemeinschaftlicher Wasch', 'Trockenraum', 'Waschküche', 'Waschmaschinenzugang'],
                        amenities):
            washing_machine = True

        pets_allowed = False
        if string_found(['Haustiere'], amenities):
            pets_allowed = True

        # END
        if "vermietet" != response.xpath('//div[@class="preisposition"]/div/@class').getall()[-1]:
            # # MetaData
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)  # String
            item_loader.add_value("external_source", self.external_source)  # String

            item_loader.add_value("external_id", external_id)  # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", remove_unicode_char(title))  # String
            item_loader.add_value("description", remove_unicode_char(" ".join(description)))  # String

            # Property Details
            item_loader.add_value("city", response.meta["city"])  # String
            item_loader.add_value("zipcode", response.meta["zipcode"])  # String
            item_loader.add_value("address", address)  # String
            item_loader.add_value("latitude", str(response.meta["lat"]))  # String
            item_loader.add_value("longitude", str(response.meta["lng"]))  # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type)  # String
            item_loader.add_value("square_meters", sqm)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            item_loader.add_value("available_date", extract_date(available_date))  # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
            # item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking)  # Boolean
            item_loader.add_value("elevator", elevator)  # Boolean
            item_loader.add_value("balcony", balcony)  # Boolean
            item_loader.add_value("terrace", terrace)  # Boolean
            # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine)  # Boolean
            # item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            item_loader.add_value("deposit", deposit)  # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities)  # Int
            item_loader.add_value("currency", "EUR")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            item_loader.add_value("heating_cost", heating_cost)  # Int

            item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            landlord_name = response.xpath(
                '//div[@class="col-pt-12 col-sm-4 col-md-3 col-md-offset-1 col-lg-3 col-lg-offset-1 rechtsexposetext"]//div[@class="kontaktname"]//text()').get()
            phone = response.xpath(
                '//div[@class="col-pt-12 col-sm-4 col-md-3 col-md-offset-1 col-lg-3 col-lg-offset-1 rechtsexposetext"]//text()').getall()
            landlord_phone = None
            for i in phone:
                if "Telefon" in i:
                    landlord_phone = i.split(":")[-1]

            item_loader.add_value("landlord_name", remove_unicode_char(landlord_name))  # String
            item_loader.add_value("landlord_phone", landlord_phone)  # String
            item_loader.add_xpath("landlord_email",
                                  '//div[@class="col-pt-12 col-sm-4 col-md-3 col-md-offset-1 col-lg-3 col-lg-offset-1 rechtsexposetext"]//a//text()')  # String

            self.position += 1
            yield item_loader.load_item()
