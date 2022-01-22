# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *

class PloehnImmobilienDeSpider(scrapy.Spider):
    name = "ploehn_immobilien_de"
    start_urls = ['http://www.ploehn-immobilien.de/index.php4?cmd=searchDetails&objq[cursor]=0&katalias=alle_objekte_wohnen&kaufartids=2,3&obercmd=search_alias_alle_objekte_wohnen_nur_miete&icmd=14483654236636']
    allowed_domains = ["ploehn-immobilien.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    header = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Length': '12',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Host': 'www.ploehn-immobilien.de',
        'Origin': 'http://www.ploehn-immobilien.de',
        'Pragma': 'no-cache',
        'Referer': 'http://www.ploehn-immobilien.de/Mietangebote.htm',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36'
    }

    # 1. SCRAPING level 1
    def start_requests(self):
        for i in range(16):
            url = f'http://www.ploehn-immobilien.de/index.php4?cmd=searchDetails&objq[cursor]={str(i)}&katalias=alle_objekte_wohnen&kaufartids=2,3&obercmd=search_alias_alle_objekte_wohnen_nur_miete&icmd=14483654236636'
            yield scrapy.Request(url, callback=self.populate_item, headers=self.header)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # Images
        raw_data = response.xpath('//*[@id="objektbildcarousel"]/div[*]/@style').extract()
        raw_images = [re.search(r'immobilien(.*).jpg', i)[0] for i in raw_data]
        images = ['http://www.ploehn-immobilien.de/'+i for i in raw_images]

        # Title
        title = response.css('h1::text').get()

        # description
        description = ' '.join(response.css('.active p::text').extract())
        description = re.sub(r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})',
                             '', description)
        description = re.sub(
            r'[\S]+\.(net|com|org|info|edu|gov|uk|de|ca|jp|fr|au|us|ru|ch|it|nel|se|no|es|mil)[\S]*\s?', '',
            description)
        description = re.sub(r'[\w.+-]+@[\w-]+\.[\w.-]+', '', description)

        description = re.sub(r"[_,.*+(){}';@#?!&$/-]+\ *", " ", description)
        description = re.sub(r"[\n\r]", " ", description)
        description = re.sub(r" +", " ", description)

        # longitude, latitude, zipcode, city, Address
        longitude, latitude = extract_location_from_address(response.css('h4+ div:nth-child(4)::text').get())
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        # rent
        rent = int(response.xpath('//*[@id="inhalt"]/div[3]/div/div[2]/div[1]/div/div/div[3]/div[4]/span[2]/text()').get().split(',')[
                       0].replace('.', ''))

        # square_meters
        square_meters = int(
            float(response.css('.col-sm-6:nth-child(2) .wert::text').get().split()[0].replace(',', '.')))

        # external_id
        external_id = response.css('.wert a::text').get()

        # Room_count
        room_count = int(float(response.css('.col-sm-6:nth-child(3) .wert::text').get()))

        # Property_type
        property_type = property_type_lookup[response.css('h4+ div:nth-child(2)::text').get()]

        # details
        labels = response.css('.col-pt-6 .key::text').extract()
        ind_1 = ind_2 = 0
        if 'bezugsfrei ab' in labels:
            ind_1 = labels.index('bezugsfrei ab')
        if 'Zustand' in labels:
            ind_2 = labels.index('Zustand')
        values = response.xpath('//*[@id="inhalt"]/div[3]/div/div[2]/div[2]/div/div[*]/div/div[2]/span/text()').extract()[0:max(ind_1, ind_2)+1] + response.css('.col-pt-6 .fa').extract() + response.xpath('//*[@id="inhalt"]/div[3]/div/div[2]/div[2]/div/div[*]/div/div[2]/span/text()').extract()[max(ind_1, ind_2)+1:]
        label_vals = dict(zip(labels, values))

        # available_date
        available_date = None
        if 'bezugsfrei ab' in label_vals.keys():
            k = label_vals['bezugsfrei ab']
            if '.' in k:
                if ' ' in k:
                    available_date = '-'.join(k.split()[0].split('.')[::-1])
                else:
                    available_date = '-'.join(k.split('.')[::-1])

        # Balcony, terrace, pets_allowed, deposit, bathroom_count, floor, heating_cost, parking, elevator
        balcony = terrace = pets_allowed = deposit = bathroom_count = floor = heating_cost = parking = elevator = None
        if 'Kaution' in label_vals.keys():
            if ',' in label_vals['Kaution']:
                deposit = int(float(label_vals['Kaution'].split(',')[0].replace('.', '')))
            elif ' ' in label_vals['Kaution']:
                deposit = int(float(label_vals['Kaution'].split()[0].replace('.', '')))
        if 'Nebenkosten (ca.)' in label_vals.keys():
            heating_cost = int(float(label_vals['Nebenkosten (ca.)'].split(',')[0].replace('.', '')))
        if 'Badezimmer' in label_vals.keys():
            bathroom_count = int(float(label_vals['Badezimmer'].replace('.', '').replace(',', '.')))

        if 'Haustiere' in label_vals.keys():
            if 'fa-check' in label_vals['Haustiere']:
                pets_allowed = True
            else:
                pets_allowed = False
        if 'Balkon' in label_vals.keys():
            if 'fa-check' in label_vals['Balkon']:
                balcony = True
            else:
                balcony = False

        if 'Terrasse' in label_vals.keys():
            if 'fa-check' in label_vals['Terrasse']:
                terrace = True
            else:
                terrace = False

        if 'Garage' in label_vals.keys():
            if 'fa-check' in label_vals['Garage']:
                parking = True
            else:
                parking = False
        if 'Anzahl Stellplätze' in label_vals.keys():
            parking = True

        if 'Etage' in label_vals.keys():
            floor = label_vals['Etage']
        if 'Aufzug' in label_vals.keys():
            elevator = True

        if 'verfügbar ab' in label_vals.keys():
            available_date = '-'.join(label_vals['verfügbar ab'].split('.')[::-1])

        # dishwasher, washing_machine
        dishwasher = washing_machine = None
        if 'Geschirrspülmaschine' in description:
            dishwasher = True
        if 'Waschmaschine' in description:
            washing_machine = True
        # Furnished
        furnished = None
        if 'Ausstattung' in label_vals.keys():
            furnished = True
        # energy_label
        energy_label = response.css('tr:nth-child(4) .wert::text').get()

        # Landlord_info
        landlord_name = response.css('.kontaktname::text').get()
        k = response.css('.col-sm-12.col-lg-12::text').extract()
        for i in k:
            if 'Telefon:' in i:
                landlord_number = i.split('Telefon:')[1].strip()
        landlord_email = response.css('.col-lg-12 a::text').get()


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
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
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
