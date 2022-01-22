import scrapy
from scrapy import Request, FormRequest
from scrapy.utils.response import open_in_browser
from scrapy.loader import ItemLoader
from scrapy.selector import Selector
from scrapy.utils.url import add_http_if_no_scheme

from ..loaders import ListingLoader
from ..helper import *
from ..user_agents import random_user_agent

import requests
import re
import time
from urllib.parse import urlparse, urlunparse, parse_qs

class RamaimmobiliareSpider(scrapy.Spider):
        
    name = 'ramaimmobiliare'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.ramaimmobiliare.com']
    post_url = 'https://www.ramaimmobiliare.com/web/immobili.asp'
    
    position = 1

    formdata = {
        "showkind": "",
        "num_page": "1",
        "group_cod_agenzia": "8075",
        "cod_sede": "0",
        "cod_sede_aw": "0",
        "cod_gruppo": "0",
        "pagref": "",
        "ref": "",
        "language": "ita",
        "maxann": "9",
        "estero": "0",
        "cod_nazione": "",
        "cod_regione": "",
        "tipo_contratto": "A",
        "cod_categoria": "R",
        "cod_tipologia": "0",
        "cod_provincia": "0",
        "cod_comune": "0",
        "prezzo_min": "",
        "prezzo_max": "",
        "mq_min": "",
        "mq_max": "",
        "vani_min": "",
        "vani_max": "",
        "camere_min": "",
        "camere_max": "",
        "riferimento": "",
        "cod_ordine": "O01",
    }
    
    def start_requests(self):
        yield FormRequest(
                        url = self.post_url, 
                        formdata = self.formdata, 
                        callback = self.parse, 
                        dont_filter = True
                        )


    def parse(self, response):
        
        
        
        cards = response.css(".property-item")
    

        for index, card in enumerate(cards):

            card_type = card.css(".for-sale::text").get()
            if "Vendita" in card_type:
                continue

            
            position = self.position
            
            property_type = "apartment"
            
            card_url = card.css("a.item-block::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)
                  
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
            }
            
            
            
            RamaimmobiliareSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
                
        nextBtn = response.css(".pulsante[rel='next']::text").get()
        if nextBtn :
            
            nextPageUrlNumber = int(self.formdata['num_page']) + 1
            prevPageUrlNumber = self.formdata['num_page']
            self.formdata['num_page'] = str(nextPageUrlNumber)
            if nextPageUrlNumber != prevPageUrlNumber:
                yield FormRequest(url = self.post_url, callback = self.parse, formdata=self.formdata, dont_filter=True)

      






    def parseApartment(self, response):


        external_id = response.css("div#det_rif::attr(data-valore)").get()
        if external_id:
            external_id = remove_white_spaces(external_id)
        
        square_meters = response.css("div#det_superficie::attr(data-valore)").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css("div#det_camere::attr(data-valore)").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
            if room_count == 0:
                room_count = response.css("div#det_vani::attr(data-valore)").get()
                room_count = remove_white_spaces(room_count)
                room_count = extract_number_only(room_count)
        else:
            room_count = response.css("div#det_vani::attr(data-valore)").get()
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)

        bathroom_count = response.css("div#det_bagni::attr(data-valore)").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)
            if bathroom_count == 0:
                bathroom_count = 1       
        else:
            bathroom_count = 1
        
        
        rent = response.css(".price::text").get()
        if rent and "Tratt. riservata" not in rent:
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
                
        currency = response.css(".price::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR" 
 
        
        title = response.css("#subheader h1::text").get()
        if title:
            title = remove_white_spaces(title)
        
                        
        province = response.css("div#det_prov::attr(data-valore)").get()
        if province:
            province = remove_white_spaces(province)
        
        area = response.css("div#det_zona::attr(data-valore)").get()
        if area:
            area = remove_white_spaces(area)
            
        city = response.css("div#det_comune::attr(data-valore)").get()
        if city:
            city = remove_white_spaces(city)
        
        street = response.css("div#det_indirizzo::attr(data-valore)").get()
        if street:
            street = remove_white_spaces(street)
            
        address = f"{province} - {area} - {city} - {street}"
            
        script_map = response.css(".map-tab iframe::attr(src)").get()
        if script_map:
            pattern = re.compile(r'maps.google.it\/maps\?f=q&q=(\d*\.?\d*),(\d*\.?\d*)&')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]
        else:
            latitude = None
            longitude = None
    
        description = response.css('.lt_content.lt_desc::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.slides li a .watermark img::attr(src)').getall()
        external_images_count = len(images)
        

        energy_label = response.css("div#det_cl_en::attr(data-valore)").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
        
        utilities = response.css("div#det_spese::attr(data-valore)").get()
        if utilities:
            utilities = remove_white_spaces(utilities)
            utilities = extract_number_only(utilities)
        
        floor = response.css("div#det_piano::attr(data-valore)").get()
        if floor:
            floor = remove_white_spaces(floor)

        elevator = response.css("div#det_ascensore::attr(data-valore)").get()
        if elevator:
            elevator = True
        else:
            elevator = False
        
            
        swimming_pool = response.css("div#det_piscina::attr(data-valore)").get()
        if swimming_pool:
            swimming_pool = True
        else:
            swimming_pool = False
            
        parking = response.css("div#det_parcheggio::attr(data-valore)").get()
        if parking:
            parking = True
        else:
            parking = False
        
        furnished = response.css("div#det_arredato::attr(data-valore)").get()
        if furnished:
            furnished = True

        else:
            furnished = False  
             
            
        balcony = response.css("div#det_balcone::attr(data-valore)").get()
        if balcony:
            balcony = True
        else:
            balcony = False   

        terrace = response.css("div#det_terrazza::attr(data-valore)").get()
        if terrace:
            terrace = True
        else:
            terrace = False    
          
        
        landlord_name = "RAMA IMMOBILIARE"
        landlord_phone = "0957210509"
        landlord_email = "info@ramaimmobiliare.com"
                
        
        if rent:
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value("address", address)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("property_type", response.meta['property_type'])
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("utilities", utilities)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("floor", floor)
            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("swimming_pool", swimming_pool)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
