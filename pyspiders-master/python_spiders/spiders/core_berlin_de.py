# -*- coding: utf-8 -*-
# Author: Prasanth
import scrapy
import re
from ..loaders import ListingLoader
from ..helper import convert_to_numeric, currency_parser, extract_location_from_address, extract_location_from_coordinates, extract_number_only, format_date


class CoreBerlin_Spider(scrapy.Spider):
    name = "core-berlin"
    start_urls = ['https://www.core-berlin.de/en/vermietung/']
    allowed_domains = ["core-berlin.de"]
    country = 'Germany' 
    locale = 'de' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        links_list=[]
        for block in response.xpath('//div[@class="jea_item"]'):
            if block.xpath('.//strong[contains(text()," Reserviert")]/text()').get('').strip() != 'Reserviert':
                links=block.xpath('./a/@href').get('')
                join_link=response.urljoin(links)
                links_list.append(join_link)
                
        for url_link in links_list:
            yield scrapy.Request(url_link, callback=self.populate_item)

    def populate_item(self, response):

        item_loader = ListingLoader(response=response)
        title=response.xpath('//h2/text()').get('')
        address=re.findall(r"'address':'(.*?)'",response.text)[0]
        latitude,longitude=extract_location_from_address(address)
        zipcode,city,address=extract_location_from_coordinates(latitude,longitude)
        floor=response.xpath('//td[contains(text(),"Floor")]/following::td/text()').get('')
        property_type=response.xpath('//h4[contains(text(),"Nutzungsart")]/text()').re_first(r"Nutzungsart:\s*(.*?)$")
        square_meters=response.xpath('//td[contains(text(),"Living space")]/following::td/text()').get('').replace(",",".").replace("m²","").strip()
        square_meters=convert_to_numeric(square_meters)
        room_count=response.xpath('//td[contains(text(),"Number of rooms")]/following::td/text()').get('')
        room_count=convert_to_numeric(room_count)
        bathroom_count=response.xpath('//td[contains(text(),"Number of bathrooms")]/following::td/text()').get('')
        bathroom_count=convert_to_numeric(bathroom_count)
        available_date_fetch=response.xpath('//h4[contains(text(),"Available from")]/text()').re_first(r"Available from\s*:\s*(.*?)$")
        available_date=format_date(available_date_fetch,"%d.%m.%Y") if re.search(r"\d+",available_date_fetch) else available_date_fetch
        parking=response.xpath('//td[contains(text(),"Anzahl Stellplätze")]/following::td/text()').get('')
        lookup= list(e.replace("&nbsp;","").lower() for e in re.findall(r'<\/i>(.*?)<br>',response.text))
        elevator=True if 'lift' in lookup else False
        balcony=True if 'balcony' in lookup else False
        terrace=True if 'terrace' in lookup else False
        swimming_pool=True if 'swimming pool' in lookup else False
        washing_machine=True if 'washing machine' in lookup else False
        dishwasher=True if 'dish washer' in lookup else False
        images=response.xpath('//div[@class="ws-titangallery"]/a/@href').getall()
        rent=response.xpath('//td[contains(text(),"Price")]/following::td/text()').get('').replace(",",".").replace("€","").strip()
        rent =''.join(rent.split('.')[:-1])
        deposit=response.xpath('//td[contains(text(),"Deposit")]/following::td/text()').get('').replace(",",".").replace("€","").strip()
        deposit =''.join(deposit.split('.')[:-1])
        currency=currency_parser("€","german")
        heating_cost=response.xpath('//td[contains(text(),"Heating Costs")]/following::td/text()').get('').replace(",",".").replace("€","").strip()
        heating_cost=convert_to_numeric(heating_cost)
        energy_label=response.xpath('//div[contains(text(),"Klasse")]/text()').re_first(r"Klasse.*?:\s*(.*?)$")
        landlord_name=response.xpath('//h3[contains(text(),"Your contact person")]/following::div/text()').get('')
        landlord_number=response.xpath('//i[@class="fa fa-phone"]/parent::div/text()').get('').replace("&nbsp;","")
        landlord_email=response.xpath('//i[@class="fa fa-envelope"]/parent::div/text()').get('').replace("&nbsp;","")
        
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int
        item_loader.add_value("available_date", available_date) # String => date_format
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        item_loader.add_value("currency", currency) # String
        item_loader.add_value("heating_cost", heating_cost) # Int
        item_loader.add_value("energy_label", energy_label) # String
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String
        self.position += 1
        yield item_loader.load_item()
    