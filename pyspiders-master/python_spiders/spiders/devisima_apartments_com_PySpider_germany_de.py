# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from ..loaders import ListingLoader
from scrapy import Request
import json

class devisima_apartments_com_PySpider_germanySpider(scrapy.Spider):
    name = "devisima_apartments_com"
    start_urls = [
    'https://devisima-apartments.com/rooms/apartment-in-berlin-prenzlauer-berg/',
    'https://devisima-apartments.com/rooms/apartment-in-moabit/'
    ]
    allowed_domains = ["devisima-apartments.com"]
    country = 'Germany' 
    locale = 'de' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

        

    def parse(self, response):
        item_loader = ListingLoader(response=response)
        title = response.css("#app > header > div > div.relative.w-full.h-full.flex.flex-col.flex-auto > div > div.relative.h-full.w-full.px-10.xl\:max-w-7xl.xl\:m-auto > h1::text").get()
        
        description = ''
        descriptions = response.css("#app > div:nth-child(2) > section.section.section--light.section--detail-page.section--small-bottom-padding > div > div.mb-16.mx-auto.lg\:w-2\/3.font-light.text-lg.text-gray-900 > div > p *::text").extract()
        for i in range(len(descriptions)):
            description = description + descriptions[i]

        furnished = None
        if 'eingerichtet' in description:
            furnished = True

        services = response.css("#app > div:nth-child(2) > section.section.section--light.section--detail-page.section--small-bottom-padding > div > div:nth-child(2) > div > div > div > span::text").extract()
        elevator = None
        if 'Personenaufzug' in services:
            elevator = True
    
        room_count = None
        bathroom_count = None
        if 'Bad mit Dusche' in services:
            bathroom_count = 1
        square_meters = int(response.css("#app > div:nth-child(2) > div.bg-modulebackground > div > div > div:nth-child(2) > span::text").get().split(" ")[0])
        if square_meters < 50:
            room_count = 1
        else:
            room_count = None

        rent = response.css("#app > div:nth-child(2) > div.bg-modulebackground > div > div > div:nth-child(3) > span::text").get().split('ab')[1].split('€')[0]
        if ',' in rent:
            rent = int(rent.split(',')[0])
        else:
            rent = int(rent)

        images = []
        imagess = response.css("body").get().split('"filesize"')[1].split('","link"')[0]
        imagez = imagess.split('"url":"')[1].replace('\\','')
        images.append(imagez)
        external_images_count = len(images)
        
        item_loader.add_value("external_link", response.url) 
        item_loader.add_value("external_source", self.external_source) 

        item_loader.add_value("position", self.position) 
        item_loader.add_value("title", title) 
        item_loader.add_value("description", description)
        item_loader.add_value('city','berlin')
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", square_meters) 
        item_loader.add_value("room_count", room_count) 
        item_loader.add_value("bathroom_count", bathroom_count) 
        item_loader.add_value("furnished", furnished) 
        item_loader.add_value("elevator", elevator) 
        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images))
        item_loader.add_value("rent", rent) 
        item_loader.add_value("currency", "EUR") 
        item_loader.add_value("landlord_name", "Devisima Apartments")
        item_loader.add_value("landlord_phone", "(030) 319 90 70")
        item_loader.add_value("landlord_email", "kontakt@devisima-apartments.com") 

        self.position += 1
        yield item_loader.load_item()
