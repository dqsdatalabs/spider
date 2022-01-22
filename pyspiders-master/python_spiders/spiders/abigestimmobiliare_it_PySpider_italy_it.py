import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json

class abigestimmobiliare_it_PySpider_italySpider(scrapy.Spider):
    name = 'abigestimmobiliare_it'
    allowed_domains = ['abigestimmobiliare.it']
    start_urls = [
        'http://www.abigestimmobiliare.it/?page_id=7'
        ]
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):  
        urls = response.css("#right_side > div > div.result_image > a::attr(href)").extract()
        for url in urls:
            yield Request(url=url,
            callback = self.parse_property)

    def parse_property(self, response):
        item_loader = ListingLoader(response=response)
        description = response.css("#description > p:nth-child(3)::text").get()
        images = response.css("#multiple_images > div > a::attr(href)").extract()
        for i in range(len(images)):
            images[i]=images[i].replace(" ","%20")
        images.pop()
        external_images_count = len(images)
        rent = response.css("#search_query > h2").get().split("€")[1].split(" ")[0]
        if '.' in rent:
            rent = int(rent.replace(".",""))
        else:
            rent = int(rent)
        title = response.css("head > title::text").get()
        if "INDUSTRIALE" not in title and "Ufficio" not in title and "commerciale" not in title:
            rooms = response.css("#description > ul > li").extract()
            external_id = None
            bathroom_count = None
            room_count = None
            floor = None
            square_meters = None
            available_date = None
            elevator = None
            parking = None
            
            address = response.css("#search_query > p:nth-child(3) > span::text").get()
            
            for i in range(len(rooms)):
                if "Riferimento Immobile" in rooms[i]:
                    external_id = rooms[i].split("</strong>\r\n\t\t\t\t\t\t\t")[1].strip().split("<")[0].strip()
                if "Disponibile da" in rooms[i]:
                    available_date = rooms[i].split("</strong>\r\n")[1].strip().split("<")[0].strip()

                try:
                    if "Area calpestabile" in rooms[i]:
                        square_meters = int(rooms[i].split("</strong>\r\n")[1].strip().split("<")[0].strip())
                except:
                    pass

                if "Bagni" in rooms[i]:
                    bathroom_count = int(rooms[i].split("</strong>\r\n")[1].strip().split("<")[0].strip())
                if "Camere da letto" in rooms[i]:
                    room_count = int(rooms[i].split("</strong>\r\n")[1].strip().split("<")[0].strip())
                if "Ascensore" in rooms[i]:
                    elevator = rooms[i].split("</strong>\r\n")[1].strip().split("<")[0].strip()
                if "Garage" in rooms[i]:
                    parking = rooms[i].split("</strong>\r\n")[1].strip().split("<")[0].strip()
                if "Piano" in rooms[i]:
                    floor = rooms[i].split("</strong>\r\n")[1].strip().split("<")[0].strip()
            if "Appartamento" in title:
                property_type = 'apartment'
            else:
                property_type = 'house'
            if "No" in parking:
                parking = False
            else:
                parking = True
            if "No" in elevator:
                elevator = False
            else:
                elevator = True
            balcony = None
            pets_allowed = None
            try:
                if "balconi" in description:
                    balcony = True 

                if "No animali" in description:
                    pets_allowed = False
            except:
                pass
            title = title.replace('| Abigest Immobiliare','')
            city = address.split('|')[1].strip()
            
            if room_count == 0:
                room_count = 1
            if bathroom_count == 0:
                bathroom_count = None
            if 100 < rent < 1300:
                if 'commerciale' not in description.lower():
                    item_loader.add_value('external_link', response.url)        
                    item_loader.add_value('external_id',external_id)
                    item_loader.add_value('external_source', self.external_source)
                    item_loader.add_value('title',title)
                    item_loader.add_value('description',description)
                    item_loader.add_value('city',city)
                    item_loader.add_value('address',address)
                    item_loader.add_value('property_type',property_type)
                    item_loader.add_value('square_meters',square_meters)
                    item_loader.add_value('room_count',room_count)
                    item_loader.add_value('bathroom_count',bathroom_count)
                    item_loader.add_value('available_date',available_date)
                    item_loader.add_value('images',images)
                    item_loader.add_value('external_images_count',external_images_count)
                    item_loader.add_value('rent',rent)
                    item_loader.add_value('currency','EUR')
                    item_loader.add_value('floor',floor)
                    item_loader.add_value('parking',parking)
                    item_loader.add_value('elevator',elevator)
                    item_loader.add_value('balcony',balcony)
                    item_loader.add_value('pets_allowed',pets_allowed)
                    item_loader.add_value('landlord_name','abigestimmobiliare')
                    item_loader.add_value('landlord_phone','0775.824311')
                    # item_loader.add_value('landlord_email','0775.824311')
                    yield item_loader.load_item()

