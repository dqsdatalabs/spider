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

class StimacasaSpider(scrapy.Spider):
        
    name = 'stimacasa'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.stimacasa.com']
    start_urls = ['https://www.stimacasa.com']

    position = 1


    def parse(self, response):
        url = "https://www.stimacasa.it/ricerca-avanzata?category_ids%5B%5D=2&property_type=3&agent_type=-1&price=&keyword=&sortby=a.isFeatured&orderby=desc&address=&state_id=&postcode=&se_geoloc=&radius_search=5&isSold=0&nbath=&nbed=&nfloors=&nroom=&sqft_min=&sqft_max=&lotsize_min=&lotsize_max=&created_from=&created_to=&advfieldLists=&currency_item=&live_site=https%3A%2F%2Fwww.stimacasa.it%2F&limitstart=0&process_element=&option=com_osproperty&task=property_advsearch&show_more_div=0&Itemid=315&search_param=catid%3A2_type%3A3_type%3A3_country%3A92_sortby%3Aa.isFeatured_orderby%3Adesc&list_id=0&adv_type=3&show_advancesearchform=1&advtype_id_1=3&advtype_id_2=3&advtype_id_3=3&advtype_id_4=3&advtype_id_5=3&advtype_id_6=3&advtype_id_8=&advtype_id_9=&advtype_id_10=&advtype_id_11="
        yield Request(url,
            callback=self.parse2,
            dont_filter=True)



    def parse2(self, response):


            
        cards = response.css(".property-list ul.margin0 > li")

        for index, card in enumerate(cards):

            position = self.position
            
            card_url = card.css(".property-desc  h4 a::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)
                  
            
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
            
            
            
            StimacasaSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            
        
        if len(cards) > 0:
            
            prev_page = int(parse_qs(response.url)['limitstart'][0])
            next_page = int(parse_qs(response.url)['limitstart'][0]) + 18
            parsed = urlparse(response.url)
            new_query = parsed.query.replace(f"&limitstart={prev_page}",f"&limitstart={next_page}")
            parsed = parsed._replace(query= new_query)
            nextPageUrl = urlunparse(parsed)
            
            if nextPageUrl:
                yield Request(url=nextPageUrl, callback=self.parse2, dont_filter=True)


    def parseApartment(self, response):

        property_type = "apartment"
            
        external_id = response.css("h1.property-header-info-name-texte span::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)
        
        square_meters = response.css(".fieldlabel:contains('Superficie') + .fieldvalue::text").getall()
        square_meters = " ".join(square_meters)
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            if square_meters:
                square_meters = square_meters.split(".")[0]
                square_meters = extract_number_only(square_meters)
            elif square_meters == "":
                square_meters = "not found"
        else: 
            square_meters = "not found"
        
        description = response.css(".description .entry-content::text, .description .entry-content p::text, .description .entry-content p strong::text").getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        
        room_count = response.css(".fieldlabel:contains('Locali') + .fieldvalue::text").getall()
        room_count = " ".join(room_count)
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = response.css(".fieldlabel:contains('Camere') + .fieldvalue::text").getall()
            room_count = " ".join(room_count)
            if room_count:
                room_count = remove_white_spaces(room_count)
                room_count = extract_number_only(room_count)
            else:
                try:
                    pattern = re.compile(r', (\d*\.?\d*) camerette,')
                    x = pattern.search(description)
                    room_count = x.groups()[0]
                except Exception as err:
                    pass

        bathroom_count = response.css(".fieldlabel:contains('Bagni') + .fieldvalue::text").getall()
        bathroom_count = " ".join(bathroom_count)
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)  
        else:
            try:
                pattern = re.compile(r', (\d*\.?\d*) bagni')
                x = pattern.search(description)
                bathroom_count = x.groups()[0]
            except Exception as err:
                pass       

        rent = response.css(".price-ribbon-price::text").get()
        if rent:
            rent = remove_white_spaces(rent).split(",")[0]
            rent = extract_number_only(rent).replace(".","")
            if rent == 0:
                rent = None
        else:
            rent = None
            
        currency = response.css(".price-ribbon-price::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
           
        
        title = response.css("h1.property-header-info-name-text::text, h1.property-header-info-name-text span::text").getall()
        title = " ".join(title)
        if title:
            title = remove_white_spaces(title)  
        
        address = response.css(".property-header-info-address::text").getall()
        address = " ".join(address)
        if address:
            address = remove_white_spaces(address)
            
        city = address.split(",")[-1]
        
        script_map = response.css("script::text").getall()
        script_map = " ".join(script_map)
        script_map = remove_white_spaces(script_map)
        if script_map:
            pattern = re.compile(r'var centerPlace = new google.maps.LatLng\((\d*\.?\d*), (\d*\.?\d*)\);')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]
        
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
 
        
        images = response.css('.mosaicflow__item  img::attr(src), .iframecolorbox img::attr(src)').getall()
        external_images_count = len(images)

 
        energy_label = response.css(".m_energy ::text").getall()
        energy_label = " ".join(energy_label)
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
        
        
        floor = response.css(".fieldlabel:contains('Abitazione posta al Piano') + .fieldvalue::text").getall()
        floor = " ".join(floor)
        if floor:
            floor = remove_white_spaces(floor)
        
        balcony = response.css(".fieldlabel:contains('Balconi') + .fieldvalue::text").getall()
        balcony = " ".join(balcony)
        if balcony:
            balcony = remove_white_spaces(balcony)    
            if balcony:
                balcony = True
            else:
                balcony = False  
        
        terrace = response.css(".fieldvalue:contains('Terrazzo')::text").getall()
        terrace = " ".join(terrace)
        if terrace:
            terrace = remove_white_spaces(terrace)    
            if terrace:
                terrace = True
            elif terrace == "":
                terrace = False
        else:
            terrace = False    

        washing_machine = response.css(".span6:contains('Lavatrice')::text").get()
        if washing_machine:
            washing_machine = True
        else:
            washing_machine = False 
        
        dishwasher = response.css(".span6:contains('Lavapiatti')::text").get()
        if dishwasher:
            dishwasher = True
        else:
            dishwasher = False 
        
                
        landlord_email = response.css(".agentbasicinformation li:last-child .right a::text").get()
        if landlord_email:
            landlord_email = landlord_email.replace(" ","")
            landlord_email = remove_white_spaces(landlord_email)
            
        landlord_phone = response.css(".agentbasicinformation li:first-child .right::text").get()
        if landlord_phone:
            landlord_phone = landlord_phone.replace(" ","")
            landlord_phone = remove_white_spaces(landlord_phone)
        
        landlord_name = response.css("h2 .edicon.edicon-user-tie + a::text").get()
        if landlord_name:
            landlord_name = landlord_name.replace(" ","")
            landlord_name = remove_white_spaces(landlord_name)

        if rent: 
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url.replace(".it/",".com/"))
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", address)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("floor", floor)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("dishwasher", dishwasher)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
