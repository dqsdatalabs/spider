# -*- coding: utf-8 -*-
# Author: Prasanth
import re
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class ImmobrokerLeipzig_Spider(scrapy.Spider):
    name = "immobroker_leipzig"
    start_urls = ['https://www.immobroker-leipzig.de/immobilien/?vermarktungsart=miete/']
    allowed_domains = ["immobroker-leipzig.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for block in response.xpath('//div[@class="property-details col-sm-12 vertical"]'):
            if 'Parkfläche' not in block.xpath('./div[@class="property-subtitle"]/text()').get(''):
                links=block.xpath('./h3/a/@href').get('')
                join_link=response.urljoin(links)
                yield scrapy.Request(join_link,callback=self.populate_item)        
        
        next_page = response.xpath('//a[@class="next page-numbers"]/@href').get('')
        if next_page:
            yield scrapy.Request(next_page,callback=self.parse)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        external_id=response.xpath('//div[contains(text(),"Objekt ID")]/following::div/text()').get('')
        title=response.xpath('//h1[@class="property-title"]/text()').get('')
        description=response.xpath('//div[@class="panel-body"]').re_first(r'Beschreibung<\/h3>\s*<p>([\w\W]*?)<h3>')
        description=re.sub(r'<.*?>','',description).replace('\n','')
        description=remove_white_spaces(description)
        address=response.xpath('//li[@class="list-group-item"]').re_first("Adresse<\/div>\s*([\w\W]*?)<\/div>")
        address=re.sub(r'<.*?>','',address).replace('\n','')
        address=remove_white_spaces(address)
        latitude,longitude=extract_location_from_address(address)
        zipcode,city,address=extract_location_from_coordinates(latitude,longitude)
        address=address if address!='04575' else '04575,Neukieritzsch,Sachsen'
        floor=response.xpath('//li[@class="list-group-item"]').re_first(r'Etage<\/div>\s*([\w\W]*?)<\/div>')
        floor=remove_white_spaces(re.sub(r'<.*?>','',floor))
        property_type=remove_white_spaces(response.xpath('//div[contains(text(),"Objekttypen")]/following::div/text()').get(''))
        property_type='apartment' if 'wohnung' in property_type else None
        square_meters=remove_white_spaces(response.xpath('//div[contains(text(),"Wohnfläche")]/following::div/text()').get('').replace(",",".").replace("m²",""))
        square_meters=int(float(square_meters))
        room_count=response.xpath('//div[contains(text(),"Zimmer")]/following::div/text()').get('')
        room_count=convert_to_numeric(room_count)
        balcony_fetch=response.xpath('//div[contains(text(),"Balkone")]/following::div/text()').get('')
        balcony=True if balcony_fetch!='' else False
        available_date_fetch=response.xpath('//div[contains(text(),"Verfügbar ab")]/following::div/text()').get('')
        available_date=format_date(available_date_fetch,"%d.%m.%Y") if re.search(r"\d+",available_date_fetch) else None
        furnished_fetch=response.xpath('//div[contains(text(),"Ausstattung")]/following::div/text()').get('')
        furnished=True if furnished_fetch=='Standard'or'gehoben' else False
        parking_fetch=remove_white_spaces(response.xpath('//div[contains(text(),"Stellplatz")]/following::div/text()').get(''))
        if parking_fetch=='':
            parking_fetch=remove_white_spaces(response.xpath('//div[contains(text(),"Stellplätze")]/following::div/text()').get(''))
        parking=True if parking_fetch!='' else False
        images=response.xpath('//div[@id="immomakler-galleria"]/a/@href').getall()  
        energy_label=response.xpath('//div[contains(text(),"Energie­effizienz­klasse")]/following::div/text()').get('')
        rent=remove_white_spaces(response.xpath('//div[contains(text(),"Nettokaltmiete")]/following::div/text()').get('').replace(",",".").replace('EUR',''))
        if rent=='':
            rent=remove_white_spaces(response.xpath('//div[contains(text(),"Kaltmiete")]/following::div/text()').get('').replace(",",".").replace('EUR','').replace('pro Monat',''))
        rent =''.join(rent.split('.')[:-1])
        deposit=remove_white_spaces(response.xpath('//div[contains(text(),"Kaution")]/following::div/text()').get('').replace(",",".").replace('EUR',''))
        deposit =''.join(deposit.split('.')[:-1])
        utilities=remove_white_spaces(response.xpath('//div[contains(text(),"Nebenkosten")]/following::div/text()').get('').replace(",",".").replace('EUR',''))
        utilities =''.join(utilities.split('.')[:-1])
        heating_cost=remove_white_spaces(response.xpath('//div[contains(text(),"Warmmiete")]/following::div/text()').get('').replace(",",".").replace('EUR',''))
        heating_cost =''.join(heating_cost.split('.')[:-1])
        heating_cost= int(heating_cost) - int(rent)
        currency=currency_parser('EUR','german')
        landlord_name=response.xpath('//div[contains(text(),"Name")]/following::div/span/text()').get('')
        landlord_number=response.xpath('//div[contains(text(),"Mobil")]/following::div/a/text()').get('')
        landlord_email=response.xpath('//div[contains(text(),"E-Mail Direkt")]/following::div/a/text()').get('')
      
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String
        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("available_date", available_date) # String => date_format
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("balcony", balcony)# Boolean
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int
        item_loader.add_value("currency", currency) # String
        item_loader.add_value("energy_label", energy_label) # String
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()