# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class A1aImmobilienDresdenDeSpider(scrapy.Spider):
    name = "1a_immobilien_dresden_de"
    start_urls = ['https://1a-immobilien-dresden.de/index.php/component/osproperty/erweiterte-suche?category_ids%5B%5D=6&property_types%5B%5D=3&keyword=&price=&address=&state_id=&sortby=a.isFeatured&orderby=desc&nbath=&nbed=&nfloors=&nroom=&sqft_min=&sqft_max=&lotsize_min=&lotsize_max=&type_objektpreis=LIKE&objektpreis=&type_kaltmiete=LIKE&kaltmiete=&type_warmmiete=LIKE&warmmiete=&type_gesamtmiete=LIKE&gesamtmiete=&type_provision=LIKE&provision=&type_nebenkosten=LIKE&nebenkosten=&type_sonstigekosten=LIKE&sonstigekosten=&type_kaution=LIKE&kaution=&type_stellplatz=LIKE&stellplatz=&type_mieteinnahmen=LIKE&mieteinnahmen=&schufa=&type_baujahr=LIKE&baujahr=&type_anzahletagen=LIKE&anzahletagen=&type_objektzustand=LIKE&objektzustand=&type_qualit%C3%A4t=LIKE&qualit%C3%A4t=&type_energietraeger=LIKE&energietraeger=&energieausweis=&energieausweistyp=&type_endenergiebedarf=LIKE&endenergiebedarf=&type_energieverbrauchskennwert=LIKE&energieverbrauchskennwert=&type_energieeffizienzklasse=LIKE&energieeffizienzklasse=&type_energieausweisausgestelltam=LIKE&energieausweisausgestelltam=&type_bezugsfrei=LIKE&bezugsfrei=&type_besichtigung=LIKE&besichtigung=&type_umzugsservice=LIKE&umzugsservice=&type_allgemeines=LIKE&allgemeines=&advfieldLists=86%2C96%2C87%2C105%2C97%2C104%2C117%2C107%2C106%2C88%2C89%2C100%2C112%2C101%2C102%2C103%2C108%2C109%2C116%2C110%2C114%2C118%2C90%2C91%2C111%2C113%2C115&currency_item=&live_site=https%3A%2F%2F1a-immobilien-dresden.de%2F&process_element=&option=com_osproperty&task=property_advsearch&show_more_div=0&Itemid=9999&search_param=catid%3A6_catid%3A6_country%3A71_sortby%3Aa.isFeatured_orderby%3Adesc&list_id=0&adv_type=0&show_advancesearchform=1&advtype_id_1=86%2C97%2C89%2C104%2C107%2C106%2C88%2C100%2C101%2C102%2C103%2C90%2C111%2C113%2C114%2C110%2C115%2C116%2C117%2C118%2C109%2C108%2C91%2C112&advtype_id_2=97%2C89%2C104%2C107%2C106%2C88%2C100%2C101%2C102%2C103%2C90%2C111%2C113%2C114%2C110%2C115%2C116%2C117%2C118%2C109%2C108%2C91%2C112&advtype_id_3=96%2C87%2C97%2C89%2C105%2C104%2C107%2C106%2C88%2C100%2C101%2C102%2C103%2C90%2C111%2C113%2C114%2C110%2C115%2C116%2C117%2C118%2C109%2C108%2C91%2C112&advtype_id_4=86%2C96%2C87%2C97%2C89%2C105%2C104%2C107%2C106%2C88%2C100%2C101%2C102%2C103%2C90%2C111%2C113%2C114%2C110%2C115%2C116%2C117%2C118%2C109%2C108%2C91%2C112&advtype_id_5=86%2C97%2C89%2C104%2C107%2C106%2C88%2C100%2C101%2C102%2C103%2C90%2C111%2C113%2C114%2C110%2C115%2C116%2C117%2C118%2C109%2C108%2C91%2C112&advtype_id_6=96%2C87%2C97%2C89%2C105%2C104%2C107%2C106%2C88%2C100%2C101%2C102%2C103%2C90%2C111%2C113%2C114%2C110%2C115%2C116%2C117%2C118%2C109%2C108%2C91%2C112']
    allowed_domains = ["1a-immobilien-dresden.de"]
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
        for url in ['https://1a-immobilien-dresden.de'+i for i in response.xpath('//*[@id="listings"]/div[2]/div/ul[*]/li/div[3]/h4/a/@href').extract()]:
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # longitude, latitude, zipcode, city, address
        longitude, latitude = extract_location_from_address(response.xpath('//*[@id="tm-main-top"]/div/div/div/main/div[3]/div[1]/div/div[2]/div/div[2]/div[1]/text()').get().strip())
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        # title
        title = response.css('.uk-active span::text').get().strip()

        # external_id
        external_id = response.css('.detail-title-h1 span::text').get().replace('\t', '').replace('\n', '').strip()

        # rent
        rent = int(float(response.xpath('//*[@id="currency_div"]/text()').get().strip().split(',')[0].split('€ ')[1]))

        # images
        images = response.xpath('//*[@id="tm-main-top"]/div/div/div/main/div[3]/div[1]/div/div[1]/div/div/div/figure/figcaption/div[*]/a[1]/@href').extract()

        # description
        description = response.xpath('//*[@id="detailstab"]/div[1]/div/p/text()').get()

        # details_1
        details_1 = [j for j in [i.replace('\t', '').replace('\n', '').replace('\xa0', '').strip() for i in response.css('#detailstab .span4 .span12::text').extract()] if j!='']

        # room_count, square_meters, parking
        room_count = 1
        square_meters = parking = None
        for i in details_1:
            if 'Zimmer' in i:
                room_count = int(float(i.split(': ')[1]))
            if 'Wohnfläche' in i:
                square_meters = int(float(i.split(': ')[1].split()[0].replace(',', '')))
            if 'platz' in i or 'plätze' in i:
                parking = True

        # details_2
        details_2 = [j for j in [i.replace('\t', '').replace('\n', '').replace('\xa0', '').strip() for i in response.css('.nopadding .span4::text').extract()] if j!='']

        furnished = None
        if len(details_2):
            furnished = True

        # terrace, balcony, pets_allowed, elevator, washing_machine, parking
        terrace = balcony = pets_allowed = elevator = washing_machine = None
        for i in details_1:
            if 'platz' in i or 'plätze' in i or 'platz' in title or 'platz' in description.lower():
                parking = True
            if 'Terrasse' in i or 'Terrasse' in title or 'terrasse' in description.lower():
                terrace = True
            if 'Balkon' in i or 'Balkon' in title or 'balkon' in description.lower():
                balcony = True
            if 'Haustier' in i:
                pets_allowed = True
            if 'Waschmaschine' in i or 'Waschmaschine' in title or 'waschmaschine' in description.lower():
                washing_machine = True
            if 'Fahrstuhl/Aufzug' in i or 'Aufzug' in title or 'aufzug' in description.lower():
                elevator = True

        # details_3
        details_3 = [j for j in [i.replace('\t', '').replace('\n', '').replace('\xa0', '').strip() for i in response.css('#detailstab .span6::text').extract()] if j != '']

        # deposit, available_date, floor, utilities, energy_label, heating_cost
        deposit = available_date = floor = utilities = energy_label = heating_cost = None
        for i in details_3:
            if 'Gesamtmiete' in i:
                utilities = int(float(i.split(':')[1].replace('€','').replace('.', '').replace(',', '.'))) - rent
            if 'Nebenkosten' in i:
                heating_cost = int(float(i.split(':')[1].replace('€','').replace(',', '.')))
            if 'Kaution' in i:
                deposit = int(float(i.split(':')[1].split()[0].replace('€','').replace('.', '').replace(',', '.')))*rent
            if 'Bezugsfrei ab' in i:
                if '.' in i:
                    available_date = '-'.join(i.split(':')[1].split('.')[::-1])
            if 'Etagen' in i:
                floor = i.split(':')[1][0]
            if 'Effizienzklasse' in i:
                energy_label = i.split(':')[1][0]

        # property_type
        property_type = 'apartment'
        if 'wohnung' in description.lower() or 'wohnung' in title.lower():
            property_type = 'apartment'
        elif 'haus' in description.lower() or 'haus' in title.lower():
            property_type = 'house'


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
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
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
        item_loader.add_value("landlord_name", 'SILKO PROTZE') # String
        item_loader.add_value("landlord_phone", '0177 - 6110467') # String
        item_loader.add_value("landlord_email", 's.protze@1a-immobilien-dresden.de') # String

        self.position += 1
        yield item_loader.load_item()

