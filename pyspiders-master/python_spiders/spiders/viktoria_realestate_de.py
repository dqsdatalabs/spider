# -*- coding: utf-8 -*-
# Author: LoGALINGAM
import re
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address,extract_location_from_coordinates,convert_to_numeric,currency_parser

class ViktoriaRealestate_Spider(scrapy.Spider):
    name = "viktoria_realestate"
    start_urls = ['http://www.viktoria-realestate.com/Angebote.htm']
    allowed_domains = ["viktoria-realestate.com"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1
    page_count = 1
    next_page_urls = []
    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse,cb_kwargs={'page_count':self.page_count})

    # 2. SCRAPING level 2
    def parse(self, response, page_count):
        property_urls = list(set(list(filter(bool,[response.urljoin(e) for e in response.xpath('//div[@class="infiniteresults"]/div/div/a/@href').extract()],))))
        for property_url in property_urls: 
            yield scrapy.Request(property_url,self.populate_item)
        if page_count == 1:
            self.next_page_urls = list(set(list(filter(bool,[response.urljoin(e) for e in response.xpath('//div[@class="blaetternavigation"]/ul/li/a/@href').extract()],))))

        for count,next_page in enumerate(self.next_page_urls):
            if 'Angebote.htm' not in next_page:
                page_count+=1
                self.next_page_urls.pop(count)
                yield scrapy.Request(next_page,callback=self.parse,cb_kwargs={'page_count':page_count})
                
    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String
        item_loader.add_value("position", self.position) # Int
        
        
        title = next(iter(filter(bool,[e for e in response.xpath('//div[@class="col-pt-12"]/h1//text()').extract()],)),"")
        description = ' '.join(list(set(list(filter(bool,[e for e in response.xpath('//div[contains(@class,"textausstattung")]/p//text()').extract()])))))
        address = next(iter(filter(bool,[e for e in response.xpath('//h4[contains(text(),"Adresse")]/following::div//text()').extract()],)),"")
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        property_type = next(iter(filter(bool,[e for e in response.xpath('//h4[contains(text(),"Objektart")]/following::div//text()').extract()],)),"")
        floor = next(iter(filter(bool,[e for e in response.xpath('//span[contains(text(),"Etage")]//parent::div//following-sibling::div//span//text()').extract()],)),"")
        square_meters = next(iter(filter(bool,[e for e in response.xpath('//span[contains(text(),"Wohnfläche")]/parent::div/span[@class="wert"]//text()').extract()],)),"")
        square_meters = int(float(square_meters.replace('m²','').replace(',','.')))
        room_count = next(iter(filter(bool,[e for e in response.xpath('//span[contains(text(),"Zimmer")]/parent::div/span[@class="wert"]//text()').extract()],)),"")
        bathroom_count = next(iter(filter(bool,[e for e in response.xpath('//span[contains(text(),"Badezimmer")]//parent::div//following-sibling::div//span//text()').extract()],)),"")
        balcony_text = next(iter(filter(bool,[e for e in response.xpath('//span[contains(text(),"Balkon")]/text()').extract()],)),"")
        terrace_text = next(iter(filter(bool,[e for e in response.xpath('//span[contains(text(),"Terrasse")]/text()').extract()],)),"")
        elevator_text = next(iter(filter(bool,[e for e in response.xpath('//span[contains(text(),"Aufzug")]/text()').extract()],)),"")
        parking_text = next(iter(filter(bool,[e for e in response.xpath('//span[contains(text(),"Anzahl Stellplätze")]/text()').extract()],)),"")
        images = list(set(list(filter(bool,[e for e in re.findall(r'url\((.*?\.jpg)\)',response.text,re.DOTALL)],))))
        rent_currency = next(iter(filter(bool,[e for e in response.xpath('//span[contains(text(),"Kaltmiete")]//parent::div//following-sibling::span[@class="wert"]//text()').extract()],)),"")
        if rent_currency:
                
            rent = re.sub(r'\.[\d+]*?\,\-\s*','',rent_currency)
            rent = int(float(rent.replace('.000,- ','').replace(',- ','').replace('€','').replace('.','')))  
            currency = currency_parser(rent_currency,self.country)
            deposit = next(iter(filter(bool,[e for e in response.xpath('//span[contains(text(),"Kaution")]/parent::div/following::div[1]/span[@class="wert"]/text()').extract()],)),"")
            deposit = convert_to_numeric(deposit.replace(',- ','').replace('€','').replace('.',''))
            utilities = next(iter(filter(bool,[e for e in response.xpath('//span[contains(text(),"Nebenkosten")]/parent::div/following::div[1]/span[@class="wert"]/text()').extract()],)),"")
            utilities = convert_to_numeric(utilities.replace(',- ','').replace('€','').replace('.',''))
            energy_label = next(iter(filter(bool,[e for e in response.xpath('//td[contains(text(),"Befeuerung")]/parent::tr/td[@class="wert"]/text()').extract()],)),"")
            landlord_name = next(iter(filter(bool,[e for e in response.xpath('//div[@class="kontaktname"]//text()').extract()],)),"")
            landlord_number = next(iter(filter(bool,[e for e in re.findall('Telefon\s*:\s*(.*?)<br\/>',response.text)],)),"")
            landlord_email = next(iter(filter(bool,[e for e in re.findall('<a\s*href\=\'mailto\:\s*([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)',response.text)],)),"")
            property_type = property_type.lower()
            
            property_type = 'apartment' if 'wohnung' == property_type.lower() or 'etagenwohnung' == property_type or 'erdgeschosswohnung' == property_type or 'dachgeschosswohnung' == property_type else 'house'
                    
            room_count = room_count if room_count else '1'
            balcony = True if balcony_text else False
            terrace = True if terrace_text else False
            elevator = True if elevator_text else False
            parking = True if parking_text else False
            
            item_loader.add_value("title", title) # String
            item_loader.add_value("description", description) # String
            # # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", str(latitude)) # String
            item_loader.add_value("longitude", str(longitude)) # String
            item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", int(float(room_count.replace(',','.')))) # Int
            item_loader.add_value("bathroom_count",convert_to_numeric(bathroom_count)) # Int
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            item_loader.add_value("terrace", terrace) # Boolean
            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            item_loader.add_value("deposit", deposit) # Int
            item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", currency) # String
            item_loader.add_value("energy_label", energy_label) # String
            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_number) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()