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

class Buonaparte48Spider(scrapy.Spider):

    name = 'buonaparte48'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    
    allowed_domains = ['www.buonaparte48.com']

    position = 1
    def start_requests(self):
        start_urls = [
            {'url': 'https://www.buonaparte48.com/ita/immobili?order_by=price&company_id=&seo=&luxury=&categories_id=&rental=1&property_type_id=1&property_subtype_id=&city_id=&price_max=&size_min=&size_max=&page=1',
            'property_type': 'apartment'},
            {'url': 'https://www.buonaparte48.com/ita/immobili?order_by=price&company_id=&seo=&luxury=&categories_id=&rental=1&property_type_id=11&property_subtype_id=&city_id=&price_max=&size_min=&size_max=&page=1',
            'property_type': 'apartment'},
            {'url': 'https://www.buonaparte48.com/ita/immobili?order_by=price&company_id=&seo=&luxury=&categories_id=&rental=1&property_type_id=1001&property_subtype_id=&city_id=&price_max=&size_min=&size_max=&page=1',
            'property_type': 'apartment'},
            {'url': 'https://www.buonaparte48.com/ita/immobili?order_by=price&company_id=&seo=&luxury=&categories_id=&rental=1&property_type_id=128&property_subtype_id=&city_id=&price_max=&size_min=&size_max=&page=1',
            'property_type': 'house'},
        ]
        
        for url in start_urls:
            yield Request(url=url.get('url'), callback=self.parse, dont_filter=True, meta={'property_type': url.get('property_type')})

    def parse(self, response):
        
        cards = response.css("#immobili-elenco .item")
        

        for index, card in enumerate(cards):

            position = self.position
            
            property_type = response.meta['property_type']
            
            card_url = card.css(" a.detail::attr(href)").get()

            external_id = response.css("a.detail .feature ul.detail li span:contains('Rif')::text").get()
            if external_id:
                external_id = external_id.split(" ")[1]
            
            square_meters = card.css("a.detail .feature ul.detail li span:contains('mq')::text").get()
            if square_meters:
                square_meters = square_meters.split(" ")[0]

            bathroom_count = card.css("a.detail .feature ul.detail li:nth-of-type(3) span.value::text").get()
            if bathroom_count:
                bathroom_count = convert_string_to_numeric(bathroom_count, Buonaparte48Spider)
            else:
                bathroom_count = 1 
                
            
            rent = card.css(".left-el .price::text").get()
            if rent:
                rent = convert_string_to_numeric(rent, Buonaparte48Spider)

            currency = card.css(".left-el .price::text").get()
            if currency:
                currency = currency_parser(currency, self.external_source)
                
            city = card.css(".location::text").getall()[1]
            if city:
                city = remove_white_spaces(city)

            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
                "external_id": external_id,
                "square_meters": square_meters,
                "bathroom_count": bathroom_count,
                "rent": rent,
                "currency": currency,
                "city": city,
            }


            Buonaparte48Spider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        if len(cards) > 0:
            prev_page = int(parse_qs(response.url)['page'][0])
            next_page = int(parse_qs(response.url)['page'][0]) + 1
            parsed = urlparse(response.url)
            new_query = parsed.query.replace(f"&page={prev_page}", f"&page={next_page}")
            parsed = parsed._replace(query = new_query)
            nextPageUrl = urlunparse(parsed)
            
            if nextPageUrl:
                yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True, meta = {'property_type': response.meta['property_type']})
        else:
            pass
        

    def parseApartment(self, response):

        room_count = response.css("[title='Locali'] + b::text").get()
        if room_count:
            room_count = extract_number_only(room_count)
        else:
            room_count = 1

        
        script_map = response.css("#tab-map > script::text").get()
        if script_map:
            pattern = re.compile(r'var fenway = new google.maps.LatLng\((\d*\.?\d*),(\d*\.?\d*)\);')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]

        
        address = response.css("h2.title.dettaglio::text").get()
        if address:
            address = address

        
        description = response.css('#content1 .description::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)

        title = response.css("h2.title.dettaglio::text").get()
        if title:
            title = title.strip()
            
        energy_label = response.css("[title='Classe Energ.'] + b::text").get()
        if energy_label:
            energy_label = energy_label.strip()

        images = response.css('.images_list li img::attr(src)').getall()
        external_images_count = len(images)
        floor_plan_images = response.css('#content3 a::attr(href)').getall()

        utilities = response.css("[title='Spese Annuali'] + b::text").get()
        if utilities:
            utilities = int(extract_number_only(utilities)) // 12

        
        elevator = response.css("[title='Ascensore'] + b::text").get()
        if elevator:
            elevator = True

        furnished = response.css("[title='Arredato'] + b::text").get()
        if furnished:
            furnished = True
        
        balcony = response.css("[title='Balcone/i'] + b::text").get()
        if balcony:
            balcony = True

        terrace = response.css("[title='Terrazzo/i'] + b::text").get()
        if terrace:
            terrace = True


        
        landlord_email = "info@buonaparte48.com"
        landlord_phone = "0283424595"
        landlord_name = "buona parte 48"



        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.meta['external_id'])
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("city", response.meta['city'])
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("property_type", response.meta['property_type'])
        item_loader.add_value("square_meters", response.meta['square_meters'])
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", response.meta['bathroom_count'])
        item_loader.add_value("images", images)
        item_loader.add_value("floor_plan_images",floor_plan_images)
        item_loader.add_value("external_images_count", external_images_count)
        item_loader.add_value("rent", response.meta['rent'])
        item_loader.add_value("currency", response.meta['currency'])
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
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
