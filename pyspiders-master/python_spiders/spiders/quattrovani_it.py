# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class QuattrovaniSpider(Spider):
    name = 'quattrovani_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.https://quattrovani.it"]
    start_urls = ["https://www.quattrovani.it/affitti/"]

    def parse(self, response):
        for url in response.css("div.rh_list_card__details h3 a::attr(href)").getall():
            yield Request(url=url, callback=self.populate_item, dont_filter=True)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "Apartment"
        rent = response.css("div.rh_page__property_price p.price::text").get().split(" Al ")[0].strip()
        title = response.css("div.rh_page__property_title h1.rh_page__title::text").get().strip()
        
        array_of_data = response.css("div.rh_property__meta span.figure::text").getall()
        square_meters = array_of_data[2].strip()
        try:
            square_meters = int(square_meters)
            if(square_meters > 1000 ):
                square_meters = int(array_of_data[1].strip())

            if( square_meters < 30 ):
                square_meters = int(array_of_data[3].strip())
        except ValueError:
            pass
        
        images = response.css("div.flexslider ul.slides li a img::attr(src)").getall()
        room_count = response.css("div.rh_property__row:nth-child(2) > div:nth-child(1) > div:nth-child(2) > span:nth-child(2)::text").get()
        energy_label = response.css("div.rh_property__common_note p::text").get()

        description = response.css("div.rh_content p span::text").getall()
        description = " ".join(description)
        external_id = response.css("div.rh_property__id p.id::text").get()
        
        bathroom_count = response.css("div.rh_property__meta:nth-child(2) > div:nth-child(2) > span:nth-child(2)::text").get().strip()
        if int(bathroom_count) > 3:
            bathroom_count = response.css("div.rh_property__row:nth-child(2) > div:nth-child(1) > div:nth-child(2) > span:nth-child(2)::text").get()
        
        
        parking = response.css("div.rh_property__meta:nth-child(8) > div:nth-child(2) > span:nth-child(2)::text").get()
        if parking:
            parking = True
        else:
            parking = False
        
        
        landlord_name = "Agente Rossella Puglisi"
        landlord_phone = "3486583995"
        landlord_email = "info@quattrovani.it"

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("title", title)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("images", images)
        item_loader.add_value("room_count", int(room_count))
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("parking", parking)
        item_loader.add_value("description", description)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("external_id", external_id)
       
        yield item_loader.load_item()