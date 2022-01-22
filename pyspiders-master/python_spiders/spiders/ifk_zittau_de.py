# -*- coding: utf-8 -*-
# Author: sri ram 
import scrapy
from ..loaders import ListingLoader
from ..helper import *



class IfkZittauDe_Spider(scrapy.Spider):
    name = "ifkzittau"   
    start_urls = ['https://www.ifk-zittau.de/mietwohnungen-zittau/']
    allowed_domains = ["ifk-zittau.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        links = response.xpath('//div[@class="propbox clearfix"]//a/@href').getall()
        for link in links:
            yield scrapy.Request(link,callback=self.populate_item)

            
    def populate_item(self, response):
        
        address = response.xpath('//div[@class="propaddress"]/p/text()').get('')
        longitude, latitude = extract_location_from_address(address)
        zipcode,city,address = extract_location_from_coordinates(longitude,latitude)
        title = response.xpath('//h1[@class="pagetitle"]/text()').get('')
        description= list(remove_white_spaces(e) for e in (response.xpath('//div[@id="tab1"]/p//text()').extract()))
        description = ' '.join(description)
        property_type = response.xpath('//i[@class="icofont-building-alt"]/parent::div/text()').get('')
        if 'wohnung' in property_type.lower():
            property_type = 'apartment'
        elif ' etagenwohnung' in property_type.lower():
            property_type = 'apartment'
        else:
            property_type = ''

        room_count = response.xpath('//td[contains(text(),"Zimmer:")]/following::td/text()').get('')
        square_meters = response.xpath('//td[contains(text(),"Wohnfläche:")]/following::td/text()').get('')
        square_meters = extract_last_number_only(square_meters)
        rent = response.xpath('//td[contains(text()," Miete: ")]/following::td/text()').get('')
        rent = extract_last_number_only(rent)
        deposit = response.xpath('//td[contains(text()," Kaution: ")]/following::td/text()').get('')
        deposit = extract_last_number_only(deposit)
        currency =currency_parser("€","german")
        heating_cost = response.xpath('//td[contains(text()," Heizkosten: ")]/following::td/text()').get('')
        heating_cost =extract_last_number_only(heating_cost)
        utilities= response.xpath('//td[contains(text()," Nebenkosten: ")]/following::td/text()').get('')
        utilities = extract_last_number_only(utilities)
        energy_label = response.xpath('//td[contains(text()," Energieeffizienzklasse: ")]/following::td/text()').get('')
        bathroom_count = response.xpath('//i[@class="icofont-bathtub"]/parent::div/text()').get('')
        bathroom_count = extract_last_number_only(bathroom_count)
        if not bathroom_count:
            bathroom_count = ''
        available_date = response.xpath('//td[contains(text(),"Verfügbar ab:")]/following::td/text()').get('')
        if re.search(r'\d+',available_date):
            available_date=format_date(available_date,"%d.%m.%y")
        else:
            available_date = None   
        images = response.xpath('//img[@class="pic"]/@src').getall()
        heating_cost = response.xpath('//td[contains(text()," Heizkosten: ")]/following::td/text()').get('')
        heating_cost = extract_last_number_only(heating_cost)
        landlord_name = response.xpath('//span[contains(text(),"Markus Horn")]/text()').get('')
        landlord_number = response.xpath('//span[@class="profilemobile"]//a/text()').get('')
        landlord_email = response.xpath('//a[@itemprop="email"]/text()').get('')
        balcony_in_apartment = response.xpath('//div[@class="propfeatures"]/span[contains(text(),"Balkon")]/text()').get('')
        parking_in_apartment= response.xpath('//div[@class="propfeatures"]/span[contains(text()," Stellplatz/Carport")]/text()').get('')
        balcony = True if 'balkon' in balcony_in_apartment.lower() else False
        parking = True if 'Stellplatz/Carport' in parking_in_apartment.lower() else False
        washing_machine = True if 'waschmaschine' in description.lower() else False


        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url) 
        item_loader.add_value("external_source", self.external_source) 
        item_loader.add_value("position", self.position)
        item_loader.add_value("title", title) 
        item_loader.add_value("description", description) 
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode) 
        item_loader.add_value("address", address) 
        item_loader.add_value("latitude", str(latitude)) 
        item_loader.add_value("longitude", str(longitude)) 
        item_loader.add_value("property_type", property_type) 
        item_loader.add_value("square_meters", int(float(square_meters)))
        item_loader.add_value("room_count", int(room_count))
        item_loader.add_value("available_date", available_date) 
        item_loader.add_value("bathroom_count",(bathroom_count))
        item_loader.add_value("parking", parking) 
        item_loader.add_value("balcony", balcony) 
        item_loader.add_value("washing_machine",washing_machine)
        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images))
        item_loader.add_value("rent", int(float(rent))) 
        item_loader.add_value("deposit", int(float(deposit)))
        item_loader.add_value("currency", currency) 
        item_loader.add_value("heating_cost", int(float(heating_cost)))
        item_loader.add_value("utilities", int(float(utilities)))
        item_loader.add_value("energy_label", energy_label) 
        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_number) 
        item_loader.add_value("landlord_email", landlord_email) 
        self.position += 1
        yield item_loader.load_item()