import scrapy
from scrapy import Request, FormRequest
from scrapy.utils.response import open_in_browser
from scrapy.loader import ItemLoader
from scrapy.selector import Selector

from ..loaders import ListingLoader
from ..helper import *
from ..user_agents import random_user_agent

import requests
import re
import time
from urllib.parse import urlparse, urlunparse, parse_qs

class SarpiSpider(scrapy.Spider):
    
    name = 'sarpi'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.sarpi.it']
    start_urls = ['https://sarpi.it/cerca-immobili/?_sfm_contratto=A&_sft_categoria=immobili-residenziali&_sfm_tipologia=Appartamento-%2C-APPARTAMENTO-%2C-Attico-%2C-Bilocale-%2C-Capannone-%2C-Casa%20indipendente-%2C-CASA%20INDIPENDENTE-%2C-CASALE%20INDIPENDENTE-%2C-Loft-%2C-Rustico-%2C-Stanza%20-%20Camera-%2C-trilocale-%2C-Villa-%2C-Villa%20a%20Schiera-%2C-VILLA%20INDIPENDENTE-%2C-Villa%20Indipendente-%2C-Villetta-%2C-Villetta%20-%2C-Villetta%20a%20schiera&_sfm_prezzo=100+10000000&_sfm_mqsuperficie=0+100000']

    position = 1

    def parse(self, response):
        
        cards = response.css("#mainImmobili .immobileCompact")
        

        for index, card in enumerate(cards):

            position = self.position
            
            card_url = card.css(".preview a::attr(href)").get()
            
            property_type = card.css(".contenuto .top .baseline .el.special::text").get()
            if property_type:
                property_type = property_type_lookup.get(property_type,"apartment")
            
            city = card.css(".contenuto .doveSiTrova::text").get()
            if city:
                city = remove_white_spaces(city)
            
            
            external_id = card.css(".contenuto .dscr .rif::text").get()
            if external_id:
                external_id = external_id.split(". ")[1].strip()
            else:
                external_id = "Not Found"

            square_meters = card.css(".contenuto .top .baseline .el:contains('mq')::text").get()
            if square_meters:
                square_meters = remove_white_spaces(square_meters)
                square_meters = square_meters.split(" ")[0]
            
            room_count = response.css(".contenuto .top .baseline .el:contains('locali')::text").get()
            if room_count:
                room_count = convert_string_to_numeric(room_count, SarpiSpider)
            else:
                room_count = 1 

            bathroom_count = response.css(".contenuto .top .baseline .el:contains('Bagno')::text,.contenuto .top .baseline .el:contains('Bagni')::text").get()
            if bathroom_count:
                bathroom_count = convert_string_to_numeric(bathroom_count, SarpiSpider)
            else:
                bathroom_count = 1             

            rent = card.css(".contenuto .top .prezzo::text").get()
            if rent:
                rent = remove_white_spaces(rent)
                rent = convert_string_to_numeric(rent, SarpiSpider)

            currency = card.css(".contenuto .top .prezzo::text").get()
            if currency:
                currency = remove_white_spaces(currency)
                currency = currency_parser(currency, self.external_source)
                
            landlord_email = card.css(".contenuto .links a:contains('Email')::attr(href)").get()
            if landlord_email:
                landlord_email = landlord_email.split(":")[1]
                landlord_email = remove_white_spaces(landlord_email)
     
            landlord_phone = card.css(".contenuto .links a:contains('Chiama')::attr(href)").get()
            if landlord_phone:
                landlord_phone = landlord_phone.split(":")[1]
                landlord_phone = remove_white_spaces(landlord_phone)

            dataUsage = {
                "position": position,
                "property_type": property_type,
                "city": city,
                "card_url": card_url,
                "external_id": external_id,
                "square_meters": square_meters,
                "room_count": room_count,
                "bathroom_count": bathroom_count,
                "rent": rent,
                "currency": currency,
                "landlord_email": landlord_email,
                "landlord_phone": landlord_phone,
            }
            
            
            SarpiSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        
        nextPageUrl = response.css("#pagination .nav-links .next::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = nextPageUrl
            
        
        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter=True)
        

    def parseApartment(self, response):


        title = response.css("#headImmobile h1::text").get()
        if title:
            title = title.strip()
            
        address = response.css("#headImmobile #zoneRifHeadImmobile::text").get()
        if address:
            address = remove_white_spaces(address).replace("-","")

        
        description = response.css('#datiPrimoPiano #dscrImmobile::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)

            
        energy_label = response.css("#datiImmobile #energeticaImmobile .el.energetica b::text").get()
        if energy_label:
            energy_label = energy_label.strip()

        images = response.css('#galleriaImmobile .notMobile img::attr(src)').getall()
        external_images_count = len(images)
        
        floor_plan_images = response.css('#planimetrieImmobile img::attr(src)').getall()

        utilities = response.css("#datiImmobile #costiImmobile .el:contains('SPESE CONDOMINIALI') span::text").get()
        if utilities:
            utilities = utilities.split(".")[0].strip()
            utilities = convert_string_to_numeric(utilities, SarpiSpider)

        furnished = response.css("#caratteristicheImmobile .el:contains('ARREDAMENTO') span::text").get()
        if furnished == "Arredamento completo":
            furnished = True
        elif furnished == "Arredamento parziale":
            furnished = True
        elif furnished == "Arredamento assente":
            furnished = False
        else:
            furnished = False
        
        balcony = response.css("#caratteristicheImmobile .el:contains('AREE ACCESSORIE') ul li:contains('Balconi')::text").get()
        if balcony:
            balcony = True

        terrace = response.css("#caratteristicheImmobile .el:contains('AREE ACCESSORIE') ul li:contains('Terrazzi')::text").get()
        if terrace:
            terrace = True

        landlord_name = response.css(".linkAgenzia::text").get()
        if landlord_name:
            landlord_name = remove_white_spaces(landlord_name)


        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.meta['external_id'])
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("city", response.meta['city'])
        item_loader.add_value("address", address)
        item_loader.add_value("property_type", response.meta['property_type'])
        item_loader.add_value("square_meters", response.meta['square_meters'])
        item_loader.add_value("room_count", response.meta['room_count'])
        item_loader.add_value("bathroom_count", response.meta['bathroom_count'])
        item_loader.add_value("images", images)
        item_loader.add_value("floor_plan_images",floor_plan_images)
        item_loader.add_value("external_images_count", external_images_count)
        item_loader.add_value("rent", response.meta['rent'])
        item_loader.add_value("currency", response.meta['currency'])
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", response.meta['landlord_email'])
        item_loader.add_value("landlord_phone", response.meta['landlord_phone'])
        item_loader.add_value("position", response.meta['position'])

        yield item_loader.load_item()
        pass



def get_p_type_string(p_type_string):

    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None


def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number >= 92:
        energy_label = "A"
    elif energy_number >= 81 and energy_number <= 91:
        energy_label = "B"
    elif energy_number >= 69 and energy_number <= 80:
        energy_label = "C"
    elif energy_number >= 55 and energy_number <= 68:
        energy_label = "D"
    elif energy_number >= 39 and energy_number <= 54:
        energy_label = "E"
    elif energy_number >= 21 and energy_number <= 38:
        energy_label = "F"
    else:
        energy_label = "G"
    return energy_label
