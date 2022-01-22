# -*- coding: utf-8 -*-
# Author: Aishwarya
import scrapy
import re
from ..loaders import ListingLoader
from ..helper import remove_white_spaces, convert_to_numeric, format_date, extract_location_from_address,strip_tags



class PeterWagnerImmobilien_Spider(scrapy.Spider):
    name = "peter_wagner_immobilien"
    start_urls = ["http://www.peter-wagner-immobilien.de/"]
    allowed_domains = ["peter-wagner-immobilien.de"]
    country = "Germany"  
    locale = "de"  
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = "testing"

    position = 1

   
    def start_requests(self):
        start_urls = ["https://www.peter-wagner-immobilien.de/wohnen/"]

        for url in start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        residential_urls = response.xpath('//a[@title="Details"]/@href').getall()
        for residential_url in residential_urls:
            url = response.urljoin(residential_url)
            yield scrapy.Request(url, callback=self.populate_item)

    def populate_item(self, response):     
        external_id = response.xpath('//dt[contains(text(),"Objektnr. extern:")]/following::dd/text()').get("")
        title = response.xpath("//h1/text()").get("")
        description_block = response.xpath('//div[@class="immo-desc"]').get('')
        description = None
        if re.search('Objektbeschreibung\:([\w\W]*?)<h3\s*class\="freitext-header\s*\">Sonstiges',description_block,re.DOTALL):
            description = remove_white_spaces(strip_tags(re.findall('Objektbeschreibung\:([\w\W]*?)<h3\s*class\="freitext-header\s*\">Sonstiges',description_block,re.DOTALL)[0]))
        elif re.search('Lage\:<\/h3>\s*([\w\W]*?)<h3\s*class\="freitext-header\s*\">Sonstiges',description_block,re.DOTALL):
            description = remove_white_spaces(strip_tags(re.findall('Lage\:<\/h3>\s*([\w\W]*?)<h3\s*class\="freitext-header\s*\">Sonstiges',description_block,re.DOTALL)[0]))
        location = response.xpath('//dt[contains(text(),"Ort:")]/following::dd/text()').get("")
        city = location.split(" ")[-1]
        zipcode = location.split(" ")[0]        
        street = response.xpath('//dt[contains(text(),"Straße:")]/following::dd/text()').get("")
        address = street + " " + location
        longitude, latitude = extract_location_from_address(address)               
        floor = response.xpath('//dt[contains(text(),"Etage:")]/following::dd/text()').get("")        
        property_type = response.xpath('//dt[contains(text(),"Objektart:")]/following::dd/text()').get("").split(" ")[0]
        if "Wohnung" in property_type:
            property_type = "apartment"
        if "Haus" in property_type:
            property_type = "house"            
        square_meters = response.xpath('//dt[contains(text(),"Wohnfläche:")]/following::dd/text()').re_first(r'([^>]*?) m²')
        square_meters = square_meters.split(',')[0]          
        room_count = response.xpath('//dt[contains(text(),"Anzahl Zimmer:")]/following::dd/text()').get("")      
        if room_count == '':
            room_count = '1'  
        bathroom_count = response.xpath('//dt[contains(text(),"Anzahl Badezimmer:")]/following::dd/text()').get("")       
        available_date = response.xpath('//dt[contains(text(),"Verfügbar ab:")]/following::dd/text()').get("")       
        pets_allowed = None
        pets = response.xpath('//dt[contains(text(),"Haustiere erlaubt:")]/following::dd/text()').get("")
        if "Nein" in pets:
            pets_allowed = False
        furnished = None
        furnished = response.xpath('//h3[contains(text(),"Ausstattung:")]/text()').get("")
        if "Ausstattung" in furnished:
            furnished = True                
        parking = None
        parking = response.xpath('//span[contains(text(),"Stellplatz")]/text()').get('')
        if "Stellplatz" in parking:
            parking = True
        else:
            parking = response.xpath('//h3[contains(text(),"Ausstattung:")]/following-sibling::div').get('')
            parking = next(iter(filter(bool,(e for e in re.findall('.*(Parkflächen).*',parking,re.DOTALL)))),'')
            parking = True               
        elevator = None
        elevator = response.xpath('//span[contains(text(),"Personenaufzug")]/text()').get('')
        if "Personenaufzug" in elevator:
            elevator = True
        else:
            elevator = response.xpath('//h3[contains(text(),"Ausstattung:")]/following-sibling::div').get('')
            elevator = next(iter(filter(bool,(e for e in re.findall('.*(Aufzug).*',elevator,re.DOTALL)))),'')
            elevator = True            
        balcony = None
        balconys = response.xpath('//h3[contains(text(),"Ausstattung")]/following-sibling::div').get('')
        balcony = next(iter(filter(bool,(e for e in re.findall('.*(Balkon).*',balconys,re.DOTALL)))),'') 
        if balcony == 'Balkon':
            balcony = True  
        washing_machine = None
        washing_machine = response.xpath('//h3[contains(text(),"Ausstattung:")]/following-sibling::div').get('')
        washing_machine = next(iter(filter(bool,(e for e in re.findall('.*(Waschmaschine).*',washing_machine,re.DOTALL)))),'') 
        if washing_machine == 'Waschmaschine':
            washing_machine = True 
        images = response.xpath('//li//@src').getall()
        floor_plan_images = response.xpath('//li//img[@title="Grundriss"]/@src').getall()
        floor_plan_images = ["https://www.peter-wagner-immobilien.de"+ x for x in floor_plan_images]
        rent = response.xpath('//dt[contains(text(),"Nettokaltmiete:")]/following::dd/text()').re_first(r'([^>]*?) €')
        rent = rent.split(',')[0]       
        deposit = response.xpath('//dt[contains(text(),"Kaution:")]/following::dd/text()').re_first(r'([^>]*?) €')      
        utilities = response.xpath('//dt[contains(text(),"Nebenkosten:")]/following::dd/text()').re_first(r'([^>]*?) €')
        heating_cost = response.xpath('//dt[contains(text(),"Heizkosten:")]/following::dd/text()').re_first(r'([^>]*?) €')
        landlord_name = response.xpath('//div[@id="c1"]/p[2]/text()').get('')
        landlord_number = response.xpath('//div[@id="c1"]/p[3]/text()').get('')
        landlord_number = re.sub('Tel.: ','',landlord_number)
        landlord_email = ''.join(response.xpath('//div[@id="c1"]/p[3]/a/text()').getall())
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source) 
        item_loader.add_value("external_id", external_id) 
        item_loader.add_value("position", self.position) 
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", str(latitude)) 
        item_loader.add_value("longitude", str(longitude))
        item_loader.add_value("floor", floor)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", convert_to_numeric(room_count)) 
        item_loader.add_value("bathroom_count", convert_to_numeric(bathroom_count))
        item_loader.add_value("available_date", format_date(available_date,'%d.%m.%Y'))
        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("furnished", furnished) 
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony) 
        item_loader.add_value("washing_machine", washing_machine) 
        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images)) 
        item_loader.add_value("floor_plan_images", floor_plan_images)
        item_loader.add_value("rent", rent) 
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("currency", "EUR") 
        item_loader.add_value("heating_cost", heating_cost) 
        item_loader.add_value("utilities", utilities) 
        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_number) 
        item_loader.add_value("landlord_email", landlord_email) 

   
        self.position += 1
        yield item_loader.load_item()
