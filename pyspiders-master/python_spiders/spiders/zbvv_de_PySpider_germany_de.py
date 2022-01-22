# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_coordinates

class zbvv_de_PySpider_germanySpider(scrapy.Spider):
    name = "zbvv_de"
    start_urls = ['https://www.zbvv.de/dienstleistungen/vermietung/page/1/?keyword&offer&location=zwickau&listing-type=wohnung&bedrooms&bathrooms&min&max&orderby=date&order=desc&living_area_min&living_area_max#038;offer&location=zwickau&listing-type=wohnung&bedrooms&bathrooms&min&max&orderby=date&order=desc&living_area_min&living_area_max']
    allowed_domains = ["zbvv.de"]
    country = 'Germany'
    locale = 'de' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 
    page_number = 2
    position = 1

    
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    
    def parse(self, response, **kwargs):
        urls = response.css('.wpsight-listing-button a::attr(href)').extract()
        for i in range(len(urls)):
            yield Request(url = urls[i],
            callback=self.populate_item)
        next_page = ("https://www.zbvv.de/dienstleistungen/vermietung/page/"+ str(zbvv_de_PySpider_germanySpider.page_number)+"/?keyword&offer&location=zwickau&listing-type=wohnung&bedrooms&bathrooms&min&max&orderby=date&order=desc&living_area_min&living_area_max#038;offer&location=zwickau&listing-type=wohnung&bedrooms&bathrooms&min&max&orderby=date&order=desc&living_area_min&living_area_max")
        if zbvv_de_PySpider_germanySpider.page_number <= 6:
            zbvv_de_PySpider_germanySpider.page_number += 1
            yield response.follow(next_page, callback=self.parse)
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        external_id = response.css('.wpsight-listing-id::text').get()
        external_id = external_id.split(':')[1]
        title = response.css('.entry-title::text').get()
        description = response.css('body > div > div.wpsight-listing-section.wpsight-listing-section-description > div > p *::text').extract()
        temp = ''
        for i in range(len(description)):
            temp = temp + ' ' + description[i]
        description = temp
        temp = temp.lower()
        if 'Für weitere Informationen oder' in description:
            description = description.split('Für weitere Informationen oder')[0]

        square_meters = response.css('body > div > div.zbvv_mid_listinginfo_wrap > div:nth-child(2) > div > div > span:nth-child(1) > span.listing-details-value::text').get()
        square_meters = int(square_meters.split('m²')[0])
        latitude = response.css('body > div > div.wpsight-listing-section.wpsight-listing-section-location > div > div > meta:nth-child(2)::attr(content)').get()
        longitude = response.css('body > div > div.wpsight-listing-section.wpsight-listing-section-location > div > div > meta:nth-child(3)::attr(content)').get()
    
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        images = response.css('.size-large::attr(src)').extract()
        rent = response.css('body > div > div.zbvv_top_listinginfo_wrap > div:nth-child(1) > div > div.widget.widget_immonex_user_defined_properties_widget > div > div > span:nth-child(2) > span.listing-details-value::text').get()
        rent = rent.replace('€','').replace('\u00a0','')
        if ',' in rent:
            rent = rent.split(',')[0]
        rent = int(rent)
        deposit = None
        try:
            deposit = response.css('body > div > div.zbvv_top_listinginfo_wrap > div:nth-child(1) > div > div.widget.widget_immonex_user_defined_properties_widget > div > div > span:nth-child(5) > span.listing-details-value::text').get()
            if 'EUR' in deposit:
                deposit = deposit.replace('EUR','')
            if ',' in deposit:
                deposit = deposit.split(',')[0]
            deposit = int(deposit)
        except:
            pass
        energy_label = None
        try:
            energy_label = response.css('body > div > div.zbvv_mid_listinginfo_wrap > div:nth-child(3) > div > div > span:nth-child(4) > span.listing-details-value::text').get()
            if 'Gas' in energy_label or '.' in energy_label:
                energy_label = None
        except:
            pass
        heating_cost = response.css('body > div > div.zbvv_top_listinginfo_wrap > div:nth-child(1) > div > div.widget.widget_immonex_user_defined_properties_widget > div > div > span:nth-child(4) > span.listing-details-value::text').get()
        heating_cost = int(heating_cost.replace('€',''))

        room_count = response.css('body > div > div.zbvv_mid_listinginfo_wrap > div:nth-child(2) > div > div > span:nth-child(2) > span.listing-details-value::text').get()
        if ',' in room_count:
            room_count = room_count.split(',')[0]
        room_count = int(room_count)

        property_type = 'apartment'

        balcony = None
        elevator = None
        washing_machine = None
        pets_allowed = None
        dishwasher = None

        if 'balkon' in temp:
            balcony = True
        if 'aufzug' in temp:
            elevator = True
        if 'haustierhaltung' in temp:
            pets_allowed = True
        if 'waschmaschinenanschluss' in temp or 'waschmaschine' in temp:
            washing_machine = True
        if 'geschirrspüler' in temp:
            dishwasher = True

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

        #item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
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
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "Zentral Boden Vermietung und Verwaltung") # String
        item_loader.add_value("landlord_phone", "+49 800 36 77777") # String
        item_loader.add_value("landlord_email", "rental@zbvv.de") # String

        self.position += 1
        yield item_loader.load_item()
