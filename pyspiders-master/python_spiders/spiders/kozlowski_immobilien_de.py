# -*- coding: utf-8 -*-
# Author: Aishwarya
import scrapy
import re
from ..loaders import ListingLoader
from ..helper import remove_white_spaces
from ..helper import currency_parser
from ..helper import extract_number_only
from ..helper import convert_to_numeric
from ..helper import remove_unicode_char
from ..helper import extract_location_from_address
from ..helper import extract_location_from_coordinates


class KozlowskiImmobilien_Spider(scrapy.Spider):
    name = "kozlowski_immobilien"
    start_urls = ['http://www.kozlowski-immobilien.de/']
    allowed_domains = ["kozlowski-immobilien.de"]
    country = 'Germany' 
    locale = 'de' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        start_urls = [
        {'url': 'https://kozlowski-immobilien.de/vermietung/1-2-zimmer'},
        {'url': 'https://kozlowski-immobilien.de/vermietung/3-zimmer'},
        {'url': 'https://kozlowski-immobilien.de/vermietung/villen-haeuser'},
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),callback=self.parse)
    
    def parse(self, response, **kwargs):
        for block in response.xpath('//div[@class="property property-card single-card"]'):
            rent_status = block.xpath('.//div[@class="property-addon "]/text()').get('').strip()
            if (rent_status == '') or (rent_status == 'Unser Tipp'):
                link=response.urljoin(block.xpath('.//a/@href').get(''))
                yield scrapy.Request(link,callback=self.populate_item)
            else:
                continue
    
    def populate_item(self, response):
        external_id = response.xpath('//h1[@class="property-title"]/following-sibling::p/text()').get('')
        title = response.xpath('//h1[@class="property-title"]/span/text()').get('')
        description = next(iter(filter(bool,(remove_white_spaces(e) for e in response.xpath('//h2[contains(text(),"Objektbeschreibung")]/following-sibling::p/text()').extract()))),'')
        address_dump = response.xpath('//h1[@class="property-title"]/text()').get('')
        city = address_dump.split('-')[0]
        longitude,latitude = extract_location_from_address(address_dump) 
        zipcode,city,address = extract_location_from_coordinates(longitude,latitude)
        property_types = next(iter(filter(bool,(remove_white_spaces(e) for e in response.xpath('//h2[contains(text(),"Objektart")]/following-sibling::p/text()').extract()))),'')
        property_types = remove_unicode_char(property_types).lower()
        property_type = None
        if property_types == 'etagenwohnung':
            property_type = 'apartment'
        elif property_types == 'villa':
            property_type = 'house' 
        elif property_types =='doppelhaush lfte':
            property_type = 'house'
        square_meters = remove_white_spaces(response.xpath('//strong[contains(text(),"Wohnfläche")]/following-sibling::p/text()').get('').replace('m²',''))
        square_meters = square_meters.split(',')[0]
        square_meters = convert_to_numeric(square_meters)
        room_count = response.xpath('//strong[contains(text(),"Zimmer")]/following-sibling::p/text()').get('')
        room_count = room_count.split(',')[0]
        if room_count == '':
            room_count = '1'
        room_count = convert_to_numeric(room_count)
        bathroom_count = next(iter(filter(bool,(remove_white_spaces(e) for e in re.findall('\-\s*Badezimmer\:\s*(\d+)<br>',response.text,re.DOTALL)))),'')
        bathroom_count = convert_to_numeric(bathroom_count)
        furnished_block = response.xpath('//h2[contains(text(),"Ausstattung")]/parent::div').get()
        if furnished_block != None:
            furnished = True if furnished_block != None else False
            washing_machine = next(iter(filter(bool,(e for e in re.findall('.*(Waschmaschine).*',furnished_block,re.DOTALL)))),'')
            washing_machine = True if washing_machine != None else  False
            balcony = next(iter(filter(bool,(e for e in re.findall('.*(Balkon).*',furnished_block,re.DOTALL)))),'')        
            balcony = True if balcony != None else False
            parking = next(iter(filter(bool,(e for e in re.findall('.*(Stellplatz).*',furnished_block,re.DOTALL)))),'')  
            parking = True if parking != None else False
            terrace = next(iter(filter(bool,(e for e in re.findall('.*(terrasse).*',furnished_block,re.I)))),'')  
            terrace = True if terrace != None else False        
        elevator = next(iter(filter(bool,(e for e in re.findall('.*(LIFT).*',title,re.I)))),'') 
        elevator = True if elevator != None else False
        images = response.xpath('//div[@class="swiper-type-gallery"]//img/@src').getall()
        rent = response.xpath('//strong[contains(text(),"Kaltmiete")]/following-sibling::p/text()').get('')
        rent = extract_number_only(rent)
        rent = convert_to_numeric(rent) 
        deposit = next(iter(filter(bool,(extract_number_only(e) for e in re.findall('\-\s*Kaution\:\s*([\w\W]*?)\s*&euro;<br>',response.text,re.DOTALL)))),'')
        deposit = convert_to_numeric(deposit)
        currency = currency_parser("€","german")
        heating_cost = next(iter(filter(bool,(extract_number_only(e) for e in re.findall('\-\s*Nebenkosten\:\s*([\w\W]*?)\s*&euro;<br>',response.text,re.DOTALL)))),'')
        heating_cost = convert_to_numeric(heating_cost)
        landlord_name = next(iter(filter(bool,(e for e in re.findall('.*(KOZLOWSKI\s*IMMOBILIEN).*',response.text,re.I)))),'') 
        landlord_phone = remove_white_spaces(response.xpath('//div[@class="col-md-6"][1]//div[@class="col-sm-6"]/p/a/text()').get(''))
        landlord_phone = re.sub('\s+','',landlord_phone)
        landlord_phone = re.sub('\.','-',landlord_phone)
        landlord_email = remove_white_spaces(response.xpath('//div[@class="contact-cards"]//div[@class="col-md-6"][1]/p/a/text()').get(''))
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url) 
        item_loader.add_value("external_source", self.external_source) 
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("position", self.position) 
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("address", address)
        item_loader.add_value("zipcode", zipcode) 
        item_loader.add_value("latitude", str(latitude)) 
        item_loader.add_value("longitude", str(longitude)) 
        item_loader.add_value("city", city)
        item_loader.add_value("property_type", property_type) 
        item_loader.add_value("square_meters", square_meters) 
        item_loader.add_value("room_count", room_count) 
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("bathroom_count", bathroom_count) 
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("balcony", balcony) 
        item_loader.add_value("parking", parking) 
        item_loader.add_value("terrace", terrace)  
        item_loader.add_value("elevator", elevator)  
        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images))
        item_loader.add_value("rent", rent) 
        item_loader.add_value("deposit", deposit) 
        item_loader.add_value("currency", currency) 
        item_loader.add_value("heating_cost", heating_cost)     
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone) 
        item_loader.add_value("landlord_email", landlord_email) 

        self.position += 1
        yield item_loader.load_item()