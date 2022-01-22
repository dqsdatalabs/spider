# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class EurocityImmobilieDeSpider(scrapy.Spider):
    name = "eurocity_immobilie_de"
    start_urls = ['http://www.eurocity-immobilien.de/wohnung_miete/Wohnungen_zur_Miete_p1.html', 'http://www.eurocity-immobilien.de/wohnung_miete/Wohnungen_zur_Miete_p2.html']
    allowed_domains = ["eurocity-immobilien.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    headers = {
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9' ,
'Accept-Encoding': 'gzip, deflate' ,
'Accept-Language': 'en-US,en;q=0.9' ,
'Cache-Control': 'no-cache' ,
'Connection': 'keep-alive' ,
'Cookie': 'PHPSESSID=pq1ovms42r1n85ts9eoo71sonk; _pk_ses.1.6af4=*; _pk_id.1.6af4=50dd2e3bcf243f4a.1641566933.7.1641850729.1641850709.' ,
'Host': 'www.eurocity-immobilien.de' ,
'Pragma': 'no-cache' ,
'Upgrade-Insecure-Requests': '1' ,
'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36'
    }

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, headers=self.headers)

    # 3. SCRAPING level 3
    def parse(self, response, **kwargs):
        props = list(set(response.xpath('//*[@id="content"]/div[*]/div[2]/div[1]/a/@href').extract()))
        for url in props:
            yield scrapy.Request('http://www.eurocity-immobilien.de' + url, headers=self.headers, callback=self.populate_item)

    # 4. SCRAPING level 4
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # title
        title = response.css('h2::text').get()

        # info
        info = dict(zip(response.css('.objekt_check > b::text').extract(), [i for i in response.css('.objekt_check:nth-child(1)::text').extract() if i.strip()!='']))

        # longitude, latitude, zipcode, city, address
        longitude, latitude = extract_location_from_address(f'{info["PLZ:"]} {info["Ort:"]}')
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        if zipcode == address:
            address = address + city

        # rent, deposit, utilities, heating_cost, square_meters, room_count, available_date, warm_rent
        warm_rent = rent = deposit = utilities = heating_cost = square_meters = available_date = None
        room_count = bathroom_count = 1
        property_type = 'apartment'
        for i in info.keys():
            if 'Kaltmiete' in i:
                rent = int(float(info[i].replace(',', '.')))
            if 'Kaution' in i:
                deposit = int(float(info[i].replace(',', '.')))
            if 'Nebenkosten' in i:
                utilities = int(float(info[i].replace(',', '.')))
            if 'Heizkosten' in i:
                heating_cost = int(float(info[i].replace(',', '.')))
            if 'Warmmiete' in i:
                warm_rent = int(float(info[i].replace(',', '.')))
            if 'Wohnfläche' in i:
                if re.search(r'\d+', info[i]):
                    square_meters = int(float(re.search(r'\d+', info[i])[0].replace(',', '.')))
            if 'Kategorie' in i:
                for j in property_type_lookup.keys():
                    if j in info[i]:
                        property_type = property_type_lookup[j]
            if info[i].strip().isdigit() and len(info[i].strip())==1:
                room_count = int(info[i].strip())

            if re.search(r'\d+\.\d+\.\d+', info[i]):
                available_date = '-'.join(re.search(r'\d+\.\d+\.\d+', info[i])[0].split('.')[::-1])
        if rent is None:
            return
        # energy info
        energy_label = None
        energy_info = response.css('br+ .objekt_check::text').extract()
        for i in energy_info:
            if 'klasse' in i:
                energy_label = i.split(':')[1]

        # details
        details = ' '.join([i.strip() for i in response.css('.objekt_check+ .objekt_check::text').extract() if i.strip()!='' ])


        # description
        description = ' '.join(response.css('p::text').extract()).replace('\r\n\t','')
        description = description_cleaner(description)

        pets_allowed = furnished = parking = elevator = balcony = terrace = swimming_pool = washing_machine = dishwasher = None
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities (description, details, item_loader)

        # images
        images = ['http://www.eurocity-immobilien.de'+i for i in response.css('.vorschau_image::attr(src)').extract()]

        # landlord info
        landlord_name = [i.strip() for i in response.css('.weitere_infos+ .weitere_infos::text').extract() if i.strip() != ''][1]
        landlord_number = [i.strip() for i in response.css('.weitere_infos+ .weitere_infos::text').extract() if i.strip()!= ''][-2].split('Telefon')[1]
        landlord_email = 'werner@kuether-immobilien.de'




        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        # item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", 1) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
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
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
