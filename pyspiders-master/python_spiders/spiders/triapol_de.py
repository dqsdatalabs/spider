# -*- coding: utf-8 -*-
# Author: sri ram 

import scrapy
from scrapy import Request, FormRequest
from ..loaders import ListingLoader
from ..helper import *




class Triapol_Spider(scrapy.Spider):
    name = "triapol"
    start_urls = ['https://www.triapol.de/vermietung/']
    allowed_domains = ["triapol.de"]
    country = 'germany' 
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing' 
    position = 1


    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    
    def parse(self, response, **kwargs):
        links =response.xpath('//article[@class="object-content col_3"]/@data-href').getall()
        for link in links:
            yield scrapy.Request(link,callback=self.populate_item)


    
    def populate_item(self, response):
        
        title = response.xpath('//title/text()').get('')
        description = response.xpath('//div[@class="description"][1]/text()[1]').get('')
        description = remove_white_spaces(description)
        city = response .xpath('//a[contains(text(),"Zurück")]/following::h2/text()[1]').get('').split(',')[-1]
        zipcode = response.xpath('//dt[contains(text(),"Lage")]/following::dd/text()').get('').split(' ')[0]
        property_type = response.xpath('//div[@class="description"]/text()').extract()
        property_type = " ".join(property_type)
        if 'wohnung' in property_type.lower():
            property_type = 'apartment'
        elif 'zimmer' in property_type.lower():
            property_type = 'room'    
        else:
            property_type = ''
         
        address = response.xpath('//dt[contains(text(),"Adresse")]/following::dd/text()').get('')
        floor =response.xpath('//dt[contains(text(),"Etage")]/following::dd/text()').get('')
        square_meters = response.xpath('//dt[contains(text(),"Wohnfläche")]/following::dd/text()').get('')
        square_meters = square_meters.replace('qm','').replace(',','.')
        room_count= response.xpath('//dt[contains(text(),"Zimmer")]/following::dd/text()').get('')
        bathroom_count = response.xpath('//dt[contains(text(),"Badräume")]/following::dd/text()').get('')
        rent = response.xpath('//dt[contains(text(),"Kaltmiete")]/following::dd/text()').get('')
        rent=extract_number_only(rent)
        deposit = response.xpath('//dt[contains(text(),"Kaution")]/following::dd/text()').get('')
        deposit =extract_number_only(deposit)
        currency=currency_parser("€","german")
        heating_cost = response.xpath('//dt[contains(text(),"Warmmiete")]/following::dd/strong/text()').get('')
        heating_cost=extract_last_number_only(heating_cost)
        heating_cost = int(float(heating_cost)) - int(float(rent)) 
        utilities = response.xpath('//dt[contains(text(),"Nebenkosten")]/following::dd/text()').get('')
        utilities = extract_last_number_only(utilities)
        energy_label = response.xpath('//dt[contains(text(),"Energieeffizienzklasse")]/following::dd/text()').get('')
        images = []
        img =response.xpath('//ul[@class="slider"]//img/@src').getall()
        for x in img:
            if 'data:image/svg'not in x:
                images.append(x)
        available_date = response.xpath('//dt[contains(text(),"Frei ab")]/following::dd/text()').get('').replace('.','').strip()   
        if available_date!='':    
            available_date = format_date(available_date,'%d %B %Y')
        else:    
            available_date = None

        balcony = True if 'balkon' in title.lower()else False    
        parking = True if 'Parken' in title.lower()else False
        furnishing = response.xpath('//dt[contains(text(),"Balkon/Terasse")]/text()').get('')
        terrace = True if 'Terrasse' in furnishing.lower()else False
        landlord_name =response.xpath('//div[@class="textwidget"]/p/text()').get('')
        landlord_number = response.xpath('//span[contains(text(),"Telefon:")]/following::a/text()').get('')
        landlord_email = response.xpath('//a[@class="textBlue"]/text()').get('')

        
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("position", self.position) 
        item_loader.add_value("title", title) 
        item_loader.add_value("description", description) 
        item_loader.add_value("city", city)
        longitude, latitude = extract_location_from_address(city) 
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("address", address) 
        item_loader.add_value("latitude", str(latitude)) 
        item_loader.add_value("longitude", str(longitude)) 
        item_loader.add_value("floor", floor) 
        item_loader.add_value("property_type", property_type) 
        item_loader.add_value("square_meters",int(float(square_meters))) 
        item_loader.add_value("room_count", room_count) 
        item_loader.add_value("bathroom_count", bathroom_count) 
        item_loader.add_value("available_date", available_date) 
        item_loader.add_value("parking", parking) 
        item_loader.add_value("balcony", balcony) 
        item_loader.add_value("terrace", terrace) 
        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images)) 
        item_loader.add_value("rent",int(float(rent))) 
        item_loader.add_value("deposit",int(float(deposit)))
        item_loader.add_value("currency", currency) 
        item_loader.add_value("heating_cost",int(float( heating_cost))) 
        item_loader.add_value("utilities",int(float( utilities)))
        item_loader.add_value("energy_label", energy_label) 
        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_number) 
        item_loader.add_value("landlord_email", landlord_email) 
        self.position += 1
        yield item_loader.load_item()