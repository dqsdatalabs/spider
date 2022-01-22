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
import json

class GaiaimmobiliarefirenzeSpider(scrapy.Spider):
        
    name = 'gaiaimmobiliarefirenze'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.gaiaimmobiliarefirenze.it']
    post_url = "https://www.gaiaimmobiliarefirenze.it/web/immobili.asp"

    position = 1

    formdata = {
        "showkind": "",
        "num_page": "1",
        "group_cod_agenzia": "6232",
        "cod_sede": "0",
        "cod_sede_aw": "0",
        "cod_gruppo": "0",
        "pagref": "",
        "ref": "",
        "language": "ita",
        "maxann": "10",
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

            
        cards = response.css(".element")

        for index, card in enumerate(cards):

            position = self.position
            
            property_type = "apartment"
            
            external_id = card.css("::attr(data-rif)").get()
            if external_id:
                external_id = remove_white_spaces(external_id)
            
            card_url = card.css(".h4 a::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)
                  
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
                "external_id": external_id,
            }
            
            
            GaiaimmobiliarefirenzeSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)

        nextBtn = response.css("#col_center .pulsante::text").get()
        if nextBtn :
            nextPageUrlNumber = int(self.formdata['num_page']) + 1
            prevPageUrlNumber = self.formdata['num_page']
            self.formdata['num_page'] = str(nextPageUrlNumber)
            if nextPageUrlNumber != prevPageUrlNumber:
                yield FormRequest(url = self.post_url, callback = self.parse, formdata=self.formdata, dont_filter=True)





    def parseApartment(self, response):

        square_meters = response.css("#det_superficie::attr(data-valore)").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css("#det_vani::attr(data-valore)").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1

        bathroom_count = response.css("#det_bagni::attr(data-valore)").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)  
        else:
            bathroom_count = 1         

        rent = response.css(".prezzo::text").get()
        if rent:
            rent = rent.split(",")[0]
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css(".prezzo::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css(".padding1 .h1::text").get()
        if title:
            title = remove_white_spaces(title)
        
        
        Provincia = response.css("#det_prov::attr(data-valore)").get()
        if Provincia:
            Provincia = remove_white_spaces(Provincia)    
                     
        Comune = response.css("#det_comune::attr(data-valore)").get()
        if Comune:
            Comune = remove_white_spaces(Comune)    
            
        zona = response.css("#det_zona::attr(data-valore)").get()
        if zona:
            zona = remove_white_spaces(zona)    
            
        city = Comune
        address = f"{Provincia} - {Comune} - {zona}"
            
            
        script_map = response.css("a.mappa::attr(href)").get()
        if script_map:
            pattern = re.compile(r'll=(\d*\.?\d*),(\d*\.?\d*)&')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]
        
    
        description = response.css('.descrizione::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.sliderkit-nav-clip img::attr(src)').getall()
        external_images_count = len(images)
        

        energy_label = response.css("#det_cl_en::attr(data-valore)").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
        
        utilities = response.css("#det_spese::attr(data-valore)").get()
        if utilities:
            utilities = remove_white_spaces(utilities)
            utilities = extract_number_only(utilities)
        
        floor = response.css("#det_piano::attr(data-valore)").get()
        if floor:
            floor = remove_white_spaces(floor)

        elevator = response.css("#det_ascensore::attr(data-valore)").get()
        if elevator == "1":
            elevator = True
        else:
            elevator = False  
        
        furnished = response.css("#det_arredato::attr(data-valore)").get()
        if furnished == "1":
            furnished = True
        else:
            furnished = False  
                   
        
        landlord_email = "gaiaimmobiliarefirenze@tin.it"
        landlord_phone = "3335361009"
        landlord_name = "Gaia Immobiliare Firenze"


        if rent: 
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", response.meta['external_id'])
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
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
