import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import math
import requests

class buttonwood_ca_PySpider_canadaSpider(scrapy.Spider):
    name = 'buttonwood_ca'
    allowed_domains = ['buttonwood.ca']
    page_number = 2
    start_urls = [
        'https://buttonwood.ca/listings/'
        ]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'


    def parse(self, response):  #page_follower
        urls = response.css("#et_builder_outer_content > div > div > div > div.et_pb_section.et_pb_section_1.et_section_regular.et_section_transparent > div > div > div > div > div > div > div > div.thumb-wrapper > a::attr(href)").extract()
        availability = response.css("#et_builder_outer_content > div > div > div > div.et_pb_section.et_pb_section_1.et_section_regular.et_section_transparent > div > div > div > div > div > div > div > div.thumb-wrapper > a > span::text").extract()
        rent = response.css("#et_builder_outer_content > div > div > div > div.et_pb_section.et_pb_section_1.et_section_regular.et_section_transparent > div > div > div > div > div > div > div > div.info-wrap > div.price-area > div.price::text").extract()
        title = response.css("#et_builder_outer_content > div > div > div > div.et_pb_section.et_pb_section_1.et_section_regular.et_section_transparent > div > div > div > div > div > div > div > div.info-wrap > a::text").extract()
        for i in range(len(urls)):
            urls[i] = "https://buttonwood.ca" + urls[i]
            if "Rented" not in availability[i]:
                yield Request(url=urls[i],
                callback = self.parse_property,
                meta={
                    'rent':rent[i],
                    'title':title[i]
                })
        next_page = ("https://buttonwood.ca/listings/page/"+ str(buttonwood_ca_PySpider_canadaSpider.page_number))
        if buttonwood_ca_PySpider_canadaSpider.page_number <= 15:
            buttonwood_ca_PySpider_canadaSpider.page_number += 1
            yield response.follow(next_page, callback=self.parse)

    def parse_property(self, response):
        item_loader = ListingLoader(response=response)
        title = response.meta.get("title")
        rent = int(response.meta.get("rent").replace("$","").replace(",",""))
        description = None
        description = response.css("body > div.wrap > main > div > div > div:nth-child(2) > div.main-content-wrap.col-sm-8 > div.entry-content").get()
        description = description.replace('<div class="entry-content">','').replace('<p>','').replace('</p>','').replace('</span>','').replace('<span>','').replace('span','').replace('<','').replace('>','').replace('/','').replace('class','').replace('=','').replace('value','').replace('label','').replace('"','').replace('div','').replace('\n','')
        try:
            description = description.replace('formitem formfield','')
        except:
            pass
        info = response.css(".property-summary .value").extract()
        features = response.css("body > div.wrap > main > div > div > div:nth-child(2) > div.main-content-wrap.col-sm-8 > div.property_feature_list").get()
        property_type = info[0].split(">")[1].split("<")[0]
        if "commercial" not in property_type:
            if "condominium" in property_type or "multiplex" in property_type:
                property_type = 'apartment'
            else:
                property_type = 'house'

            details = response.css("div.main-content-wrap.col-sm-8 > div.property-details").get()
            pets_allowed = None
            if 'Pet Policy:</td>\n<td class="value">Restricted' in details:
                pets_allowed = False
            if 'Pet Policy:</td>\n<td class="value">Allowed' in details:
                pets_allowed = True
            room_count = info[3].split(">")[1].split("<")[0]
            if '+' in room_count:
                x = int(room_count.split('+')[0])
                y = int(room_count.split('+')[1])
                room_count = x+y
            bathroom_count = info[4].split(">")[1].split("<")[0]
            if '.5' in bathroom_count:
                bathroom_count = int(bathroom_count.replace('.5',''))
            furnished = info[1].split(", ")[1].split("<")[0]
            if "Unfurnished" in furnished:
                furnished = False
            else:
                furnished = True 
            parking = info[-1]
            square_meters = None
            if "sqft" in parking:
                parking = None
            if parking is not None:
                parking = parking.split(">")[1].split(",")[0]
            if parking is not None:
                parking = True
            inf = response.css(".property-details td::text").extract()
            for i in range(len(inf)):
                if "Square footage:" in inf[i]:
                    index = i
            try:
                square_meters = inf[index+1] 
            except:
                pass
            if square_meters is not None:
                square_meters = square_meters.split("sqft")[0]
                if '.' in square_meters:
                    square_meters = math.ceil(int(square_meters.split('.')[0])/10.764)
                elif '-' in square_meters:
                    square_meters = math.ceil(int(square_meters.split('-')[0])/10.764)
                else:
                    square_meters = math.ceil(int(square_meters)/10.764)

            balcony = None
            terrace = None
            washing_machine = None
            dishwasher = None
            if "balcony" in description or "balconies" in description or "Balcony" in features:
                balcony = True
            if "terrace" in description:
                terrace = True
            if "Laundry" in description or "Washer" in description or "Washer" in features:
                washing_machine = True
            if "Dishwasher" in description or "dishwasher" in features:
                dishwasher = True
            
            images = response.css("#property-carousel > div > div").extract()
            for i in range(len(images)):
                images[i] = images[i].split('<noscript><img src="')[1].split('"')[0]
            external_images_count = len(images)
            script = response.css("body > div.wrap > main > div > div > div:nth-child(2) > div.main-content-wrap.col-sm-8 > script").get()
            latlng = script.split("LatLng(")[1].split(");")[0]
            latitude = latlng.split(",")[0]
            longitude = latlng.split(",")[1]
            city = response.css("body > div.wrap > main > div > div > div:nth-child(2) > div.main-content-wrap.col-sm-8 > p > a::text").get().split("Property Management")[0]
            
            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            address = responseGeocodeData['address']['Match_addr']
            

            item_loader.add_value('external_link', response.url)        
            item_loader.add_value('external_source', self.external_source)
            item_loader.add_value('title',title)
            item_loader.add_value('description',description)
            item_loader.add_value('city',city)
            item_loader.add_value('address',address)
            item_loader.add_value('latitude',latitude)
            item_loader.add_value('longitude',longitude)
            item_loader.add_value('zipcode',zipcode)
            item_loader.add_value('property_type',property_type)
            item_loader.add_value('square_meters',int(int(square_meters)*10.764))
            item_loader.add_value('room_count',room_count)
            item_loader.add_value('bathroom_count',bathroom_count)
            item_loader.add_value('images',images)
            item_loader.add_value('external_images_count',external_images_count)
            item_loader.add_value('rent',rent)
            item_loader.add_value('currency','CAD')
            item_loader.add_value('parking',parking)
            item_loader.add_value('furnished',furnished)
            item_loader.add_value('balcony',balcony)
            item_loader.add_value('terrace',terrace)
            item_loader.add_value('washing_machine',washing_machine)
            item_loader.add_value('dishwasher',dishwasher)
            item_loader.add_value('landlord_name','buttonwood.')
            item_loader.add_value('landlord_phone','(416) 835-7191')
            item_loader.add_value('landlord_email','info@buttonwood.ca')
            yield item_loader.load_item()
            