# -*- coding: utf-8 -*-
# Author: Prasanth
import re
import scrapy
from ..helper import *
from ..loaders import ListingLoader


class KasteImmobilien_Spider(scrapy.Spider):
    name = "kaste_immobilien"
    start_urls = ['https://www.kaste-immobilien.de/immobilien?marketing_type=RENT/']
    allowed_domains = ["kaste-immobilien.de"]
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
        for block in response.xpath('//div[@class="card mb-4"]'):
            if 'Vermietet' not in remove_white_spaces(block.xpath('./h3[@class="badgeWrap"]/span/text()').get('')) and (remove_white_spaces(block.xpath('.//p[contains(text(),"€")]/text()').get(''))!='')  and (('Ladenlokal'  not in remove_white_spaces(block.css('a.text-dark::text').get('')) and ('Büro' not in remove_white_spaces(block.css('a.text-dark::text').get(''))))):          
                room_count=extract_number_only(block.xpath('.//p[contains(text(),"Schlafzimmer")]/text()').get(''))
                bathroom_count=extract_number_only(block.xpath('.//p[contains(text(),"Bäder")]/text()|.//p[contains(text(),"Bad")]/text()').get(''))
                square_meters=int(float(block.xpath('.//p[contains(text(),"m²")]/text()').get('').replace(",",".").replace("m²","")))
                rent=remove_white_spaces(block.xpath('.//p[contains(text(),"€")]/text()').get('').replace('€','').replace(',','.'))
                rent=re.sub(r'\.[\d]+$','',rent)
                links=block.xpath('./a/@href').get('')                    
                join_link=response.urljoin(links)
                yield scrapy.Request(join_link,callback=self.populate_item,meta={"room_count":room_count,"bathroom_count":bathroom_count,"square_meters":square_meters,"rent":rent}) 
        
        next_page=response.urljoin(response.xpath('//a[@class="btn btn-primary"]/@href').get(''))
        if next_page:
            yield scrapy.Request(next_page,callback=self.parse)
        
    def populate_item(self, response):
        title=response.xpath('//h1[@class="property-title"]/text()').get('')
        description=response.xpath('//h2[contains(text(),"Beschreibung")]/following::dd/p/text()').get('')
        country=response.xpath('//label[contains(text(),"Land")]/following::span/text()').get('')
        address=response.xpath('//label[contains(text(),"Ort")]/following::span/text()').get('')
        address=remove_white_spaces(address)
        combined_address="{},{}".format(address,country)
        latitude,longitude=extract_location_from_address(combined_address)
        zipcode,city,address=extract_location_from_coordinates(latitude,longitude)
        property_type=response.xpath('//label[contains(text(),"Wohnungstyp")]/following::span/text()').get('')
        property_type='apartment' if 'wohnung' in property_type else None
        square_meters=response.meta['square_meters']
        room_count=response.meta['room_count']
        room_count=room_count if room_count!='0' else 1 
        bathroom_count=response.meta['bathroom_count']
        furnished=response.xpath('//h2[contains(text(),"Ausstattung")]/text()').get('')
        furnished=True if furnished!='' else False
        balcony=True if 'balkon' in description.lower() else False
        images=response.xpath('//div[@id="image-gallery"]/div/@data-src').getall()
        rent=response.meta['rent']
        utilities=remove_white_spaces(response.xpath('//label[contains(text(),"Nebenkosten")]/following::span/text()').get('').replace('€','').replace(',','.'))
        utilities=re.sub(r'\.[\d]+$','',utilities)
        currency=currency_parser('€','german')
        landlord_name=response.xpath('//div[@class="text-left"]/strong/text()').get('')
        landlord_number=response.xpath('//div[@class="text-left"]/small/text()').getall()[-1]
        landlord_number=remove_white_spaces(landlord_number) if re.search(r'\d+',landlord_number) else ''
        landlord_email=re.findall(r'([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)',response.text)[0]
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("currency", currency) # String
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("landlord_name", landlord_name) # String'
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String
        self.position += 1
        yield item_loader.load_item()