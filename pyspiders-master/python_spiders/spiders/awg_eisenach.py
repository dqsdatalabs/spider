# -*- coding: utf-8 -*-
# Author: Omar Hammad
import scrapy, re, json, requests
from ..loaders import ListingLoader

class AwgEisenachSpider(scrapy.Spider):
    name = "awg_eisenach"
    start_urls = ['https://www.awg-eisenach.de/wohnungssuche/vermietung/wohnungsangebote/?id=355&zone=&rooms=']
    allowed_domains = ["www.awg-eisenach.de"]
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
        links = response.xpath('//*[@id="c1935"]/div/div/div/div[2]/div/a/@href').extract()
        for link in links:
            if link != '#':
                yield scrapy.Request("https://www.awg-eisenach.de"+link, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        table = response.xpath('/html/body/div[1]/div[2]/div/div[2]/div/div/div/div/div/div[5]/div[1]/div[1]/table/tbody/tr')

        rent = None 
        square_meters = None
        room_count = None
        elevator = None
        floor = None
        available_date = None
        balcony = None
        heating_cost = None
        utilities = None

        for tr in table:
            key = tr.xpath('./th/text()').extract()[0]
            value = tr.xpath('./td/text()').extract()[0]

            if key == 'Balkone:':
                balcony = True

            if key == 'Kaltmiete:':
                rent = int(float(value.split()[0].replace(',','.')))

            if key == 'Nebenkosten:':
                utilities = int(float(value.split()[0].replace(',','.')))

            if key == 'Heizkosten:':
                heating_cost = int(float(value.split()[0].replace(',','.')))
            
            if key == 'Wohnfläche:':
                square_meters = int(float(value.split()[0]))

            if key == 'Zimmer:':
                room_count = int(float(value.split()[0]))
            
            if key == 'Fahrstuhl:':
                if value != 'Nein':
                    elevator = True

            if key == 'Etage:':
                floor = value

            if key == 'Bezugsfrei ab:':
                if value != 'sofort':
                    date = value.split('.')
                    available_date = f"{date[2]}-{date[1]}-{date[0]}"

        # Get title
        title = response.xpath('/html/body/div[1]/div[2]/div/div[2]/div/div/div/div/div/h1/text()').extract()[0]

        # Get description
        desc_ps = response.css('div.container').css('div.row').css('p')
        description = ''

        for p in desc_ps:
            description += p.xpath('./text()').extract()[0]

        # Get images
        gallery = response.css(".owl-carousel img::attr('src')").extract() # Get all images in the website

        images = ["https://www.awg-eisenach.de"+link for link in gallery]

        # Get Address
        address_info = response.css('div.container').css('script').extract()[0]
        link = re.search(r'searchURL = "(.*?)"', address_info).group(1)
        info = requests.get(link).json()[0]

        latitude = info['lat']
        longitude = info['lon']

        address_info = info['display_name'].split(', ')
        address_info.pop(-1)

        zipcode = address_info.pop(-1)
        
        address = ', '.join(address_info)

        # Contact info
        contact_div = response.css('div.partner')

        landlord_name = contact_div.css('div.contactName').xpath('./strong/text()').extract()[0]
        landlord_number = contact_div.css('div.contactPhone').xpath('./a/text()').extract()[0]
        landlord_email = contact_div.css('div.contactMail').xpath('./a/text()').extract()[0]

        ############################################################################
        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        #item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", 'Eisenach') # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", "apartment") # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
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
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
