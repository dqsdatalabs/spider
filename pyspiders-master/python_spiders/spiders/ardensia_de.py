# -*- coding: utf-8 -*-
# Author: LOGALINGAM
import  re
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address,extract_location_from_coordinates,currency_parser,remove_white_spaces

class ArdensiaDeSpider(scrapy.Spider):
    name = "ardensia_de"
    start_urls = ['https://www.ardensia.de/studentenzimmer-moeblierte-appartements/']
    allowed_domains = ["ardensia.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse,meta={'property_type': 'student_apartment'})

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        property_type = response.meta['property_type']
        link_block = next(iter(filter(bool,[e for e in re.findall(r'<div\s*id\=\"myhome\-listing\-map\">[\w\W]*?<\/listing\-map>',response.text,re.DOTALL)],)),"")
        property_urls = list(set(list(filter(bool,[e.replace('&quot;','').replace('\\','').replace('&quot','').replace(':http','http') for e in re.findall(r'link(.*?)\;\,',link_block,re.DOTALL)],))))
        for property_url in property_urls:
            if 'bewerbungsformular' not in property_url:
                yield scrapy.Request(property_url,self.populate_item,meta={'property_type': property_type})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_type = response.meta['property_type']
        title = next(iter(filter(bool,[e for e in response.xpath('//h1[@class="mh-top-title__heading custom-post-h1"]//text()').extract()],)),"")
        description = list(set(list(filter(bool,[e for e in response.xpath('//div[@class="mh-estate__section mh-estate__section--details"]/p//text()').extract()],))))
        description = remove_white_spaces(' '.join(description))
        address = next(iter(filter(bool,[e for e in response.xpath('//div[@class="small-text custom-post-small-text"]//text()[2]').extract()],)),"")
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        floor = next(iter(filter(bool,[e for e in re.findall(r'\s+([\d]+)\s+Etagen',str(description),re.DOTALL)],)),"")
        square_meters = next(iter(filter(bool,[e for e in response.xpath('//strong[contains(text(),"Wohnfläche:")]/parent::li//text()[2]').extract()],)),"")
        if not square_meters:
            square_meters = next(iter(filter(bool,[e for e in re.findall('\s+([\d\,]*?)\s*m2',response.text,re.DOTALL)],)),"")
        if square_meters:
            square_meters = int(float(square_meters.replace('m²','').replace(',','.')))
        bathroom_count = next(iter(filter(bool,[e for e in re.findall(r'\s+([\d]+)\s+Bädern',str(description),re.DOTALL)],)),"")
        pet_text = next(iter(filter(bool,[e for e in response.xpath('//strong[contains(text(),"Haustiere erlaubt:")]/parent::li//text()[2]').extract()],)),"")
        furnished_text = next(iter(filter(bool,[e for e in response.xpath('//strong[contains(text(),"Möbliert")]/parent::li//text()[2]').extract()],)),"")
        images = list(set(list(filter(bool,[e for e in response.xpath('//div[@id="mh_rev_gallery_single"]/ul/li/@data-thumb').extract()],))))
        rent_currency = next(iter(filter(bool,[e for e in response.xpath('//strong[contains(text(),"Miete")]/parent::li//text()[2]').extract()],)),"")
        if not rent_currency:
            rent_currency = next(iter(filter(bool,[e for e in response.xpath('//div[@class="mh-estate__details__price"]//text()').extract()],)),"")
        rent = int(float(rent_currency.replace('ab ','').replace('€','').replace(',','.')))  
        currency = currency_parser(rent_currency,self.country)
        landlord_name = next(iter(filter(bool,[e for e in response.xpath('//strong[contains(text(),"Kontakt")]/parent::li//text()[2]').extract()],)),"")
        landlord_email = next(iter(filter(bool,[e for e in response.xpath('//strong[contains(text(),"E-Mail:")]/parent::li//text()[2]').extract()],)),"")
        landlord_number = next(iter(filter(bool,[e for e in response.xpath('//strong[contains(text(),"Telefon")]/parent::li//text()[2]').extract()],)),"")
        room_count = 1
        pets_allowed = False if 'nein' in pet_text else None
        furnished = True if ' ja' in furnished_text else False
        
        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String
        item_loader.add_value("position", self.position) # Int
        #item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("property_type", property_type) # 
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int
        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
         # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("currency", currency) # String
        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()