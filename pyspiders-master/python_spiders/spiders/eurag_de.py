# -*- coding: utf-8 -*-
# Author: Sriram
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class Eurag_Spider(scrapy.Spider):
    name = "eurag"
    start_urls = ['http://www.eurag.de/']
    allowed_domains = ["eurag.de"]
    country = 'germany'
    locale = 'de' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, dont_filter=True)

    def parse(self, response, **kwargs):
        reside_link = list(filter(bool,[response.urljoin(e) for e in response.xpath('//div[@id="wohnlnk"]/a[@title="Wohnen"]/@href').extract()]))[0]
        yield scrapy.Request(reside_link, callback=self.rent_properties, dont_filter=True)

    def rent_properties(self,response):
        properties_link = list(filter(bool,[response.urljoin(e) for e in response.xpath('//a[contains(@title," zur Miete ")]/@href').extract()]))[0]
        yield scrapy.Request(properties_link, callback=self.rent_cities, dont_filter=True)

    def rent_cities(self,response):
        cities_link = list(filter(bool,[response.urljoin(e) for e in response.xpath('//nav[contains(@class,"places block")]//li/a/@href').extract()]))

        for cities in cities_link:
            yield scrapy.Request(cities, callback=self.rent_type, dont_filter=True)

    def rent_type(self,response):
        links = list(filter(bool,[response.urljoin(e) for e in response.xpath('//li[contains(@class,"immo_category")]/a/@href').extract()]))
        for link in links:
            if link:
                yield scrapy.Request(link, callback=self.get_items, dont_filter=True)

    def get_items(self,response):
        item_urls = list(filter(bool,[response.urljoin(e) for e in response.xpath('//div[@class="object_"]//h2/a/@href').extract()]))
        for item_url in item_urls:
            if item_url:
                yield scrapy.Request(item_url, callback=self.populate_item, dont_filter=True)

        next_page = response.xpath('//span[@class="current"]/parent::li/parent::ul/li[2]/a/@href').get()
        if next_page is not None:
            next_url = response.urljoin(next_page)
            yield scrapy.Request(next_url,callback= self.get_items, dont_filter=True )

                
    def populate_item(self, response):
        title=next(iter(filter(bool,[e for e in response.xpath('//div[contains(@class,"ce_immo_object_details first")]/h1/text()').extract()])),"")
        description=response.xpath('//div[contains(@class,"detail_description")]//p/text()').extract()
        description.extend(response.xpath('//div[contains(@class,"detail_description")]//div[2]/div/text()').extract())
        description=" ".join(description)
        rent=next(iter(filter(bool,[e.replace("€","") for e in response.xpath('//div[contains(text(),"Kaltmiete")]/parent::div/div[2]/text()').extract()])),"")
        rent = re.sub(r'\,[\d+]*?\s*$','',rent)
        rent = rent.replace('.','')
        rent=int(float(rent))
        utilities = next(iter(filter(bool,[e.replace("€","") for e in response.xpath('//div[contains(text(),"Nebenkosten")]/parent::div/div[2]/text()').extract()])),"")
        utilities = re.sub(r'\,[\d+]*?\s*$','',utilities)
        utilities = utilities.replace('.','')
        utilities = int(float(utilities))
        currency=currency_parser("€","german")
        square_meters=next(iter(filter(bool,[e.replace(",",".").replace("m²","") for e in response.xpath('//div[contains(text(),"Wohnfläche")]/parent::div/div[2]/text()').extract()])),"")
        square_meters = re.sub(r'\.[\d+]*?\s*$','',square_meters)
        if square_meters:
            square_meters= int(float(square_meters))
        parking= True if 'stellplatz' in description.lower() or  'Parken' in description.lower()  else False
        balcony= True if "balkon" in title.lower() or "balkon" in description.lower() else False
        furnished= True if "ausstattung" in description.lower() else False 
        elevator= True if "aufzug" in description.lower() else False 
        dishwasher= True if "aeschirrspüler" in description.lower() else False
        room_count=next(iter(filter(bool,[e for e in response.xpath('//div[contains(text(),"Zimmer")]/parent::div/div[@class="fieldValue"]/text()').extract()])),"")
        if room_count == "":
            room_count=1
        address = list(set(list(filter(bool,[e.strip() for e in response.xpath('//div[contains(text(),"Ort")]/parent::div/div[@class="fieldValue"]/span/text()').extract()]))))
        address = address[0] +" , "+address[1]
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude,latitude)
        property_type=next(iter(filter(bool,[e for e in response.xpath('//div[contains(text(),"Kategorie")]/parent::div/div[2]/text()').extract()])),"")
        if property_type == 'Mietwohnungen':
            property_type = "apartment"
        elif property_type == "Häuser":
            property_type = "house"
        else:
            property_type = ""
        imgs = list(filter(bool,[e for e in response.xpath('//div[@class="object_images"]//@src').extract()]))
        images=[]
        for img in imgs:
            images.append(response.urljoin(img))
        energy_label=next(iter(filter(bool,[e for e in response.xpath('//div[contains(@class,"energy_efficiency_classc")]/div[2]/text()').extract()])),"")
        landlord_name = next(iter(filter(bool,[e for e in response.xpath('//div[contains(text(),"Unternehmen")]/following-sibling::div[1]/text()').extract()])),"")
        landlord_number=next(iter(filter(bool,[e.replace("\t","") for e in response.xpath('//div[contains(text(),"Telefonnummer")]/following-sibling::div[1]/a/text()').extract()])),"")
        landlord_email=next(iter(filter(bool,[e for e in response.xpath('//div[contains(text(),"E-Mail-Adresse")]/following-sibling::div/a/text()').extract()])),"")
        
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
        item_loader.add_value("square_meters", convert_to_numeric(square_meters) )
        item_loader.add_value("room_count", convert_to_numeric(room_count))
        item_loader.add_value("furnished", furnished) 
        item_loader.add_value("parking", parking) 
        item_loader.add_value("elevator", elevator) 
        item_loader.add_value("balcony", balcony) 
        item_loader.add_value("dishwasher", dishwasher) 
        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images))
        item_loader.add_value("rent", rent) 
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", currency) 
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_number) 
        item_loader.add_value("landlord_email", landlord_email) 
        
        self.position += 1
        yield item_loader.load_item()