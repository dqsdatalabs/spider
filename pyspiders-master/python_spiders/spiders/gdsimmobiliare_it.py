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

class GdsimmobiliareSpider(scrapy.Spider):

    name = 'gdsimmobiliare'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    
    allowed_domains = ['www.gdsimmobiliare.it']
    
    post_url = "https://www.gdsimmobiliare.it/wp-admin/admin-ajax.php"

    position = 1

    formdata = {
        "action": "wpestate_ajax_filter_listings",
        "action_values": "affitto",
        "category_values": "all",
        "county": "italia",
        "city": "All Cities",
        "area": "All Areas",
        "order": "2",
        "newpage": "1",
        "page_id": "5876",
        "security": "1df09c4389",
    }
    def start_requests(self):
                        
        yield FormRequest(self.post_url, callback=self.parse, formdata=self.formdata, dont_filter=True)
        pass


    def parse(self, response):
        res = response.json()['to_show']
        html = Selector(text=res)
        
        cards = html.css(".property_listing")
        
        for index, card in enumerate(cards):

            position = self.position
            card_url = card.css("::attr(data-link)").get()

            square_meters = card.css(".property_listing_details .infosize::text").get()
            if square_meters:
                square_meters = square_meters.split(" ")[0].split(".")[0]

            bathroom_count = card.css(".property_listing_details .infobath::text").get()
            if bathroom_count:
                bathroom_count = convert_string_to_numeric(bathroom_count, GdsimmobiliareSpider)
            else:
                bathroom_count = 1 
                
            
            rent = card.css(".listing_unit_price_wrapper::text").get()
            if rent:
                rent = convert_string_to_numeric(rent, GdsimmobiliareSpider)

            currency = card.css(".listing_unit_price_wrapper::text").get()
            if currency:
                currency = currency_parser(currency, self.external_source)

            dataUsage = {
                "card_url": card_url,
                "position": position,
                "square_meters": square_meters,
                "bathroom_count": bathroom_count,
                "rent": rent,
                "currency": currency,
            }

            GdsimmobiliareSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        
        
        nextPageUrlNumber = html.css(".pagination.pagination_ajax .roundright a::attr(data-future)").get()
        prevPageUrlNumber = self.formdata['newpage']
        self.formdata['newpage'] = nextPageUrlNumber
        if nextPageUrlNumber != prevPageUrlNumber:
            yield FormRequest(url = self.post_url, callback = self.parse, formdata=self.formdata, dont_filter=True)






    def parseApartment(self, response):

        property_type = "apartment"
        
        room_count = response.css("#accordion_prop_details .listing_detail:contains('Camere')::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1 

        external_id = response.css("#accordion_prop_details .listing_detail:contains('ID proprietà:')::text").get()
        if external_id:
            external_id = external_id
            
        zipcode = response.css("#accordion_prop_addr .listing_detail:contains('Cap:')::text").get()
        if zipcode:
            zipcode = zipcode
            
        latitude = response.css("#gmap_wrapper::attr(data-cur_lat)").get()
        if latitude:
            latitude = latitude
        
        longitude = response.css("#gmap_wrapper::attr(data-cur_long)").get()
        if longitude:
            longitude = longitude

        address = response.css("#accordion_prop_addr .listing_detail:contains('Indirizzo:')::text").get()
        if address:
            address = address

        city = response.css("#accordion_prop_addr .listing_detail:contains('Città:') strong + a::text").get()
        if city:
            city = city
        
        description = response.css('.wpestate_property_description > p::text, .wpestate_property_description > p strong::text').getall()
        description = " ".join(description)

        title = response.css("h1.entry-title::text").get()
        if title:
            title = title
            
        energy_label = response.css("#accordion_prop_details .listing_detail:contains('Classe energetica:')::text").get()
        if energy_label:
            energy_label = energy_label.strip()

        available_date = response.css("#accordion_prop_details .listing_detail:contains('Available')::text").get()
        if available_date:
            available_date = available_date.strip()
            available_date = format_date(available_date, date_format="%d/%m/%Y")
            
        images = response.css('#carousel-listing .item a::attr(href)').getall()
        external_images_count = len(images)


        elevator = response.css("#accordion_prop_features .listing_detail:contains('Ascensore')::text").get()
        if elevator:
            elevator = True

        furnished = response.css("#accordion_prop_features .listing_detail:contains('Arredato')::text").get()
        if furnished:
            furnished = True
        
        balcony = response.css("#accordion_prop_features .listing_detail:contains('Balcone')::text").get()
        if balcony:
            balcony = True

        terrace = response.css("#accordion_prop_features .listing_detail:contains('Terrazzo')::text").get()
        if terrace:
            terrace = True


        
        landlord_email = response.css('.agent_unit > div:nth-of-type(2) .agent_detail i.fa-envelope-o + a::text').get()
        if landlord_email:
            landlord_email = landlord_email.strip()
        
        
        landlord_phone = response.css('.agent_unit > div:nth-of-type(2) .agent_detail i.fa-phone + a::text').get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
        else: 
            landlord_phone = response.css('.agent_unit > div:nth-of-type(2) .agent_detail i.fa-mobile + a::text').get()
            if landlord_phone:
                landlord_phone = landlord_phone.strip()

        
        landlord_name = response.css('.agent_unit > div:nth-of-type(2) h4 a::text').get()
        if landlord_name:
            landlord_name = landlord_name.strip()


        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id",external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", response.meta['square_meters'])
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", response.meta['bathroom_count'])
        item_loader.add_value("available_date", available_date)
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", external_images_count)
        item_loader.add_value("rent", response.meta['rent'])
        item_loader.add_value("currency", response.meta['currency'])
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
