# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
from re import findall
import scrapy
from ..loaders import ListingLoader
from bs4 import BeautifulSoup
import urllib
import lxml
from ..helper import *
from ..items import *

class RevaSpider(scrapy.Spider):
    name = "ventoimobiliare"
    start_urls = ['http://www.venetoimmobiliare.it/res_affitto.xml']
    allowed_domains = ["www.venetoimmobiliare.it"]
    country = 'italy' # Fill in the Country's name
    locale = 'it' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development' 
    itertag = 'item'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        pass

    def bathroom_counter(self, info):
        if ("doppi servizi" in info):
            return 2
        elif (re.findall(r'bagno', info) != None):
            return len(re.findall(r'bagno', info))
    
    def room_counter(self, info):
        if ("TRICAMERE" in info or "tre camere" in info):
            return 3
        elif("BICAMERE" in info or "due camere" in info):
            return 2
        elif ("camera" in info):
            return 1
        else:
            return 0
            
    def property_getter(self, info):
        if ("VILLETTA" in info):
            return "house"
        elif("MANSARDATO" in info):
            return "studio"
        elif ("APPARTAMENTO"):
            return "apartment"
        elif ("TRICAMERE" in info):
            return "apartment"    
        elif("BICAMERE" in info):
            return "apartment" 
    def utilities_finder(self, info):
        utilities = []
        if ("lavanderia" in info or "lavatrice" in info):
            utilities.append("washing_machine")
        if("terrazze" in info or "terrazo" in info):
            utilities.append("terrace")
        if("Posto auto" in info or "POSTI AUTO" in info or "garage" in info or "Garage" in info):
            utilities.append('parking')
        return utilities
    # 3. SCRAPING level 3
    def populate_item(self, response):
        new_response = urllib.request.urlopen(self.start_urls[0])
        data = new_response.read()
        text = data.decode('utf-8')
        soup = BeautifulSoup(text,'lxml')
        items = []
        for node in soup.find_all('annuncio'):
            # # MetaData
            description = str(node.tipo.text + node.caratteristiche.text)
            item = ListingItem()
            item['external_link'] = response.url+'#'+str(self.position)
            item['external_source'] = self.external_source
            item['position'] = self.position
            item['property_type'] = self.property_getter(description)
            item['address'] = node.zona.text
            item['rent'] = int(node.prezzo.text.replace("€", "").replace(".", "").strip())
            item['currency'] = 'EUR'
            if ("NON ARREDATO" in description or "VUOTO" in description):
                item['furnished'] = False
            elif ("ARREDATO" in description):
                item['furnished'] = True
            item['room_count'] = self.room_counter(description)
            item['bathroom_count'] = self.bathroom_counter(description)
            item['description'] = node.caratteristiche.text
            long, lat = extract_location_from_address(node.zona.text)
            item['longitude'] = str(long)
            item['latitude'] = str(lat)
            zipcode, city , add = extract_location_from_coordinates(long, lat)
            item['zipcode'] = zipcode
            item['city'] = city
            for i in self.utilities_finder(description):
                if (i == 'washing_machine'):
                    item['washing_machine'] = True
                if (i == 'parking'):
                    item['parking'] = True
                if (i == 'terrace'):
                    item['terrace'] = True    
            item['landlord_name'] = "Veneto Immobiliare - contra"
            item['landlord_phone'] = "0444 546502"
            item['landlord_email'] = "info@venetoimmobiliare.it"
          

            self.position += 1
            items.append(item)
        return items
        
        
