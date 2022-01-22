import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import math
import requests

class beauxproperties_ca_PySpider_canadaSpider(scrapy.Spider):
    name = 'beauxproperties_ca'
    allowed_domains = ['beauxproperties.ca']
    start_urls = [
        'https://beauxproperties.ca/'
        ]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):
        urls = response.css("#c27-site-wrapper > div.elementor.elementor-635 > div > div > section.elementor-section.elementor-top-section.elementor-element.elementor-element-ut6fvk4.elementor-section-boxed.elementor-section-height-default.elementor-section-height-default > div > div > div > div > div > div.elementor-element.elementor-element-hp3s60y.elementor-widget.elementor-widget-case27-listing-feed-widget > div > section > div > div > div > div > div.lf-item.lf-item-default > a::attr(href)").extract()
        room = response.css("#c27-site-wrapper > div.elementor.elementor-635 > div > div > section.elementor-section.elementor-top-section.elementor-element.elementor-element-ut6fvk4.elementor-section-boxed.elementor-section-height-default.elementor-section-height-default > div > div > div > div > div > div.elementor-element.elementor-element-hp3s60y.elementor-widget.elementor-widget-case27-listing-feed-widget > div > section > div > div > div > div > div.listing-details-3.c27-footer-section > ul > li:nth-child(1) > span::text").extract()
        bath = response.css("#c27-site-wrapper > div.elementor.elementor-635 > div > div > section.elementor-section.elementor-top-section.elementor-element.elementor-element-ut6fvk4.elementor-section-boxed.elementor-section-height-default.elementor-section-height-default > div > div > div > div > div > div.elementor-element.elementor-element-hp3s60y.elementor-widget.elementor-widget-case27-listing-feed-widget > div > section > div > div > div > div > div.listing-details-3.c27-footer-section > ul > li:nth-child(2) > span::text").extract()
        for i in range(len(urls)):
            room_count = int(room[i].split('beds')[0])
            bathroom_count = bath[i].split('bathrooms')[0]
            if ".5" in bathroom_count:
                bathroom_count = int(math.ceil(float(bathroom_count)))
            else:
                bathroom_count = int(bathroom_count)
            yield Request(url=urls[i],
            callback = self.parse_property,
            meta={
                'bathroom_count':bathroom_count,
                'room_count':room_count
            })
       
    def parse_property(self, response):
        
        item_loader = ListingLoader(response=response)
        
        room_count = response.meta.get('room_count')
        bathroom_count = response.meta.get('bathroom_count')
        title = response.css(".no-rating .case27-primary-text::text").get().strip()
        description = response.css("div:nth-child(1) > div > div.pf-body > p").get()
        if 'Call us today at' in description:
            description = description.split(' Call us today at')[0]

        info = response.css("#listing_tab_rental-description > div > div > div:nth-child(1)").get()
        info_2 = response.css("#c27-site-wrapper > script").get()
        info_3 = response.css("a::attr(href)").extract()

        latitude = info_2.split('"latitude": "')[1].split('"')[0]
        longitude = info_2.split('"longitude": "')[1].split('"')[0]
        address = info_2.split('"address": "')[1].split('"')[0]
        city = address.split(', ')[1].split(',')[0]
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']

        rent = int(info.split('<h5>Rent per month from</h5>')[1].split('<p></p><p>')[1].split('</p>')[0])
        image = []
        floor_plan_images = []
        for i in range(len(info_3)):
            if '.jpg' in info_3[i]:
                if '-66-Broadway-' not in info_3[i]:
                    image.append(info_3[i])
            if '.gif' in info_3[i]:
                floor_plan_images.append(info_3[i])
        images = list(set(image))
        external_images_count = len(images)
        if external_images_count == 0:
            for i in range(len(info_3)):
                if '.jpg' in info_3[i]:
                    if '-66-Broadway-' in info_3[i]:
                        images.append(info_3[i])
            external_images_count = len(images)
    
        elevator = None
        washing_machine = None
        swimming_pool = None
        parking = None
        balcony = None

        if 'Elevators' in info:
            elevator = True
        if 'Laundry' in info:
            washing_machine = True
        if 'Pool' in info:
            swimming_pool = True
        if 'Parking' in info:
            parking = True
        if 'balconies' in description:
            baclcony = True

        property_type = 'apartment'

        leased = None
        try:
            leased = response.css(".info_outline+ span::text").get().lower()
        except:
            pass
        if leased is None:
            item_loader.add_value('external_link', response.url)        
            item_loader.add_value('external_source', self.external_source)
            item_loader.add_value('title',title)
            item_loader.add_value('description',description)
            item_loader.add_value('city',city)
            item_loader.add_value('zipcode',zipcode)
            item_loader.add_value('address',address)
            item_loader.add_value('latitude',latitude)
            item_loader.add_value('longitude',longitude)
            item_loader.add_value('property_type',property_type)
            item_loader.add_value('room_count',bathroom_count)
            item_loader.add_value('bathroom_count',bathroom_count)
            item_loader.add_value('images',images)
            item_loader.add_value('floor_plan_images',floor_plan_images)
            item_loader.add_value('external_images_count',external_images_count)
            item_loader.add_value('rent',rent)
            item_loader.add_value('currency','CAD')
            item_loader.add_value('parking',parking)
            item_loader.add_value('elevator',elevator)
            item_loader.add_value('swimming_pool',swimming_pool)
            item_loader.add_value('washing_machine',washing_machine)
            item_loader.add_value('landlord_name','beauxproperties')
            item_loader.add_value('landlord_phone','674-501-7368')
            item_loader.add_value('landlord_email', 'BeauxRentals@gmail.com')
            # item_loader.add_value(,)
            yield item_loader.load_item()
