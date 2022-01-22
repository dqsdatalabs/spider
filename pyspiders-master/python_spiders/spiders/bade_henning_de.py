# -*- coding: utf-8 -*-
# Author: Prasanth
import scrapy
from ..helper import *
from ..loaders import ListingLoader

class BadeHenning_Spider(scrapy.Spider):
    name = "bade_henning"
    start_urls = ['https://www.bade-henning.de/angebote/mietangebote/']
    allowed_domains = ["bade-henning.de"]
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
        for block in response.xpath('//div[@class="col-sm-12 col-md-6 col-lg-4"]'):
            if ('Büros' not in remove_white_spaces(block.xpath('.//span[@class="field-sesimmotool field-objektart"]/text()').get(''))) and ('Einzelhandel'not in remove_white_spaces(block.xpath('.//span[@class="field-sesimmotool field-objektart"]/text()').get(''))):
                links=block.xpath('.//a/@href').get('')
                join_link=response.urljoin(links)
                yield scrapy.Request(join_link,callback=self.populate_item)   
                     
        
        next = response.xpath('//span[contains(text(),"»")]/parent::a/@onclick').re_first(r'\d+')
        next_page=f'https://www.bade-henning.de/angebote/mietangebote/{next}'
        if next is not None:
            yield scrapy.Request(next_page,callback=self.parse)      

    def populate_item(self, response):
        title=response.xpath('//div[@class="imo-nav-title"]/h1/text()').get('')
        description=list(remove_white_spaces(e)for e in (response.xpath('//span[@class="field-sesimmotool field-texte_beschreibung"]/text()').getall()))
        location=response.xpath('//span[@class="field-sesimmotool field-ort"]/text()').get('')
        postcode=response.xpath('//span[@class="field-sesimmotool field-plz"]/text()').get('')
        address="{},{}".format(location,postcode)
        latitude,longitude=extract_location_from_address(address)
        zipcode,city,address=extract_location_from_coordinates(latitude,longitude)
        floor=response.xpath('//span[@class="field-sesimmotool field-boden"]/text()').get('')
        property_type=response.xpath('//span[@class="field-sesimmotool field-objektart"]/text()').get('')
        property_type='apartment' if property_type=='Wohnung' else None
        square_meters=int(float(response.xpath('//span[@class="field-sesimmotool field-wohnflaeche"]/text()').get('').replace(",",".").replace("m²","").replace("ca.","")))
        room_count=int(float(response.xpath('//span[@class="field-sesimmotool field-anzahl_zimmer"]/text()').get('').replace(",",".")))
        room_count_bed=response.xpath('//span[@class="field-sesimmotool field-anzahl_schlafzimmer"]/text()').get('') 
        room_count=room_count_bed if room_count=='' else room_count
        bathroom_count=response.xpath('//span[@class="field-sesimmotool field-anzahl_badezimmer"]/text()').get('')
        available_date_fetch=response.xpath('//span[@class="field-sesimmotool field-verfuegbar_ab_freitext"]/text()').get('').replace('oder früher','')
        available_date=format_date(available_date_fetch,"%d.%m.%Y") if re.search(r"\d+",available_date_fetch) else ''
        furnished=response.xpath('//th[contains(text(),"Ausstattung")]').get('')
        furnished=True if furnished!='' else False
        parking=response.xpath('//span[@class="field-sesimmotool field-stellplatzart"]/text()').get('')
        parking=True if parking!='' else False
        elevator=response.xpath('//span[@class="field-sesimmotool field-fahrstuhl"]/text()').get('')
        elevator=True if elevator!='' else False
        balcony=response.xpath('//span[@class="field-sesimmotool field-anzahl_balkon_terrassen"]/text()').get('')
        balcony=True if balcony!='' else False
        terrace=response.xpath('//span[@class="field-sesimmotool field-anzahl_balkon_terrassen"]/text()').get('')
        terrace=True if terrace!='' else False
        images=list(f"https://www.bade-henning.de{e}" for e in (response.xpath('//img[@data-target="#carousel_1"]/@src').getall()))
        floor_plan_images=list(f"https://www.bade-henning.de{e}" for e in (response.xpath('//img[@title="Grundriss"]/@src').getall()))
        images=[i for i in images if i not in floor_plan_images]
        rent=remove_white_spaces(response.xpath('//span[@class="field-sesimmotool field-kaltmiete"]/text()').get('').replace("EUR","").replace(",","."))
        rent =''.join(rent.split('.')[:-1])
        deposit=remove_white_spaces(response.xpath('//span[@class="field-sesimmotool field-kaution"]/text()').get('').replace("EUR","").replace(",","."))
        deposit =''.join(deposit.split('.')[:-1])
        currency=currency_parser("EUR","german")
        utilities=remove_white_spaces(response.xpath('//span[@class="field-sesimmotool field-nebenkosten"]/text()').get('').replace("EUR","").replace(",","."))
        utilities =''.join(utilities.split('.')[:-1])
        energy_label=response.xpath('//span[@class="field-sesimmotool field-energiepass_wertklasse"]/text()').get('')
        landlord_name=response.xpath('//div[@id="sppb-addon-1560928616396"]//p/strong/text()').get('')
        landlord_number=re.findall(r'Tel.:\s*(.*?)<br\s*\/>',response.text)[0]
        landlord_email=re.findall(r'([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)',response.text)[0]
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String
        item_loader.add_value("position", self.position)  # Int
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
        item_loader.add_value("bathroom_count", bathroom_count) # Int
        item_loader.add_value("available_date", available_date) # String => date_format
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("images", images) # Array
        item_loader.add_value("currency", currency)
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String
        item_loader.add_value("energy_label", energy_label) # String
        item_loader.add_value("landlord_name", landlord_name) # String'
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String
        self.position += 1
        yield item_loader.load_item()