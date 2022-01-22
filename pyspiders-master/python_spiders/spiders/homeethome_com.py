# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class HomeethomeSpider(Spider):
    name = 'homeethome_com'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.homeethome.com"]
    start_urls = ["https://www.homeethome.com/?advanced_city&advanced_area&filter_search_action%5B0%5D=affitto&filter_search_type%5B0%5D&superficie-minima&locali&price_low=0&price_max=300000000000&submit=RICERCA%20PROPRIET%C3%80&wpestate_regular_search_nonce=da3cc98981&_wp_http_referer=%2F"]

    def parse(self, response):
        for url in response.css("div.property_listing h4 a::attr(href)").getall():
            yield Request(url=url, callback=self.populate_item, dont_filter=True)

    def valid_property(self, property_detail):
        lower_case_detail = property_detail.lower()
        if (
            ("commerciale" in lower_case_detail) 
            or ("ufficio" in lower_case_detail) 
            or ("magazzino" in lower_case_detail) 
            or ("box" in lower_case_detail) 
            or ("auto" in lower_case_detail) 
            or ("negozio" in lower_case_detail) 
            or ("vendita" in lower_case_detail) ):
            return False
        else:
            return True

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "apartment"

        property_ad = response.css("div.property_title_label:nth-child(1) > a:nth-child(1)::text").get()
        if(property_ad != "Affitto"):
            return
        
        title = response.css("div.notice_area h1.entry-title::text").get()
        valid = self.valid_property(title)
        if( not valid):
            return
        
        property_label = response.css("div.property_title_label:nth-child(2) > a:nth-child(1)::text").get()
        valid = self.valid_property(property_label)
        if( not valid):
            return
        rent = response.css("div.price_area::text").get()
        
        address_parts = []
        first_part_of_address = response.css("div.property_categs::text").getall()[1].strip()
        address_parts.append(first_part_of_address)
        rest_of_address = response.css("div.property_categs a::text").getall()
        for part in rest_of_address:
            address_parts.append(part)
        address = " ".join(address_parts)

        square_meters = response.css("#details > div:nth-child(3)::text").get()
        try:
            square_meters = square_meters.split(".")[0]
        except:
            pass

        if(int(square_meters) < 40):
            square_meters = re.sub("\D", "", title)

        room_count = response.css("#details > div:nth-child(4)::text").get()

        images = response.css("ol.carousel-indicators li a img::attr(src)").getall()
        images = [re.sub(r'-\d*x\d*', "", img) for img in images]

        landlord_name = "homeethome"
        landlord_email = "info@homeethome.com"
        landlord_phone = "+39 011 374 4641"

        description = response.css("#description > p::text").getall()
        description = " \n ".join(description)

        bathroom_count = response.css("ul.overview_element:nth-child(4) > li:nth-child(2)::text").get()
        if(bathroom_count):
            bathroom_count = bathroom_count.split(" ")[0]

        city = response.css("#address > div:nth-child(3) > a:nth-child(2)::text").get()
        zipcode = response.css("#address > div:nth-child(4)::text").get()
        external_id = response.css("#propertyid_display::text").get()

        utilities = None
        heating_cost = None
        furnished = None
        elevator = None
        balcony = None
        floor = None

        listed_data = response.css("div.listing_detail").getall()
        
        for item in listed_data:
            if("Spese condominiali €/mese:" in item):
                utilities = item.split('<strong>Spese condominiali €/mese:</strong>')[1].split("</div>")[0]
            
            if("Spese riscaldamento €/anno:" in item):
                heating_cost = item.split("<strong>Spese riscaldamento €/anno:</strong>")[1].split("</div>")[0]
            
            if("Arredamento:" in item):
                furnished = item.split("<strong>Arredamento:</strong>")[1].split("</div>")[0]
            
            if("Ascensore:" in item):
                elevator = item.split("<strong>Ascensore:</strong>")[1].split("</div>")[0]
            
            if("Balcone:" in item):
                balcony = item.split("<strong>Balcone:</strong>")[1].split("</div>")[0]
            
            if("Piano:" in item):
                floor = item.split("<strong>Piano:</strong>")[1].split("</div>")[0]

        if(heating_cost):
            heating_cost = int(int(heating_cost) / 12)
        
        if(furnished):
            if( furnished == "No"):
                furnished = False
            else: 
                furnished = True
        
        if(elevator):
            if( elevator == "Sì"):
                elevator = True
            else: 
                elevator = False

        if(balcony):
            if( balcony == "Sì"):
                balcony = True
            else: 
                balcony = False
           
        latitude = response.css("div#gmap_wrapper::attr(data-cur_lat)").get()
        longitude = response.css("div#gmap_wrapper::attr(data-cur_long)").get()
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("address", address)
        item_loader.add_value("title", title)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("description", description)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        if utilities:
            item_loader.add_value("utilities", utilities)
        if heating_cost:
            item_loader.add_value("heating_cost", heating_cost)
        if furnished:
            item_loader.add_value("furnished", furnished)
        if elevator:
            item_loader.add_value("elevator", elevator)
        if balcony:
            item_loader.add_value("balcony", balcony)
       
        yield item_loader.load_item()
