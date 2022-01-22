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

class AcropolisrealestateSpider(scrapy.Spider):
        
    name = 'acropolisrealestate'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.acropolisrealestate.it']
    start_urls = ['https://www.acropolisrealestate.it/properties-search/?type%5B%5D=residenziale&status=affitto&sortby=price-asc']

    position = 1


    def parse(self, response):
        
        
        cards = response.css(".rh_page__listing .rh_list_card")
        

        for index, card in enumerate(cards):

            position = self.position
            
            property_type = "apartment"
            
            card_url = card.css(".rh_list_card__details h3 a::attr(href)").get()

                

            
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
            }

            
            
            AcropolisrealestateSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        
        nextPageUrl = response.css(".rh_pagination a.current + a::attr(href)").get()

        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)
      





    def parseApartment(self, response):


        external_id = response.css("p.id::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)
        
        square_meters = response.css(".prop_area div span::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".prop_bedrooms div span::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)

        bathroom_count = response.css(".prop_bathrooms div span::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)           

        rent = response.css("p.price::text").get()
        if rent:
            rent = rent.split(",")[0]
            rent = extract_number_only(rent).replace(".","")
            
        currency = response.css("p.price::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css("h1.rh_page__title::text").get()
        if title:
            title = remove_white_spaces(title)
        
        address = response.css("p.rh_page__property_address::text").get()
        if address:
            address = remove_white_spaces(address)
                        
        city = address.split(" ")[-1]
        if city:
            city = remove_white_spaces(city)
        
        zipcode = address.split(" ")[-2]
        if zipcode:
            zipcode = remove_white_spaces(zipcode)
            
        script_map = response.css("#property-google-map-js-extra::text").get()
        if script_map:
            pattern = re.compile(r',"lat":"(\d*\.?\d*)","lng":"(\d*\.?\d*)",')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]
        
    
        description = response.css('.rh_content p::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('[data-fancybox="gallery"]::attr(href)').getall()
        external_images_count = len(images)
        
        floor_plan_images = response.css('[data-fancybox="floor-plans"]::attr(href)').getall()

        energy_label = response.css(".epc-details li strong:contains('energetica') + span::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
          

        elevator = response.css(".rh_property__features  li.rh_property__feature a:contains('Ascensore')::text").get()
        if elevator:
            elevator = True
        else:
            elevator = False
             
            
        balcony = response.css(".rh_property__features  li.rh_property__feature a:contains('Balcone')::text").get()
        if balcony:
            balcony = True
        else:
            balcony = False   

        terrace = response.css(".rh_property__features  li.rh_property__feature a:contains('Terrazzo')::text").get()
        if terrace:
            terrace = True
        else:
            terrace = False    
        
        washing_machine = response.css(".rh_property__features  li.rh_property__feature a:contains('Lavatrice')::text,.rh_property__features  li.rh_property__feature a:contains('Lavanderia')::text").get()
        if washing_machine:
            washing_machine = True
        else:
            washing_machine = False    
        
        landlord_email = "info@acropolisrealestate.it"
        landlord_phone = "3887573766"
        landlord_name = "Acropolis Real Estate"
                
        

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("property_type", response.meta['property_type'])
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("images", images)
        item_loader.add_value("floor_plan_images",floor_plan_images)
        item_loader.add_value("external_images_count", external_images_count)
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("position", response.meta['position'])

        yield item_loader.load_item()
