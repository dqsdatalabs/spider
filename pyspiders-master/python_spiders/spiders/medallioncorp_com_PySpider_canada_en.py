import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import requests

class medallioncorp_com_PySpider_canadaSpider(scrapy.Spider):
    name = 'medallioncorp_com'
    allowed_domains = ['medallioncorp.com']
    page_number = 2
    start_urls = [
        'https://medallioncorp.com/properties/page/1/'
        ]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'



    def parse(self, response):  #page_follower
        urls = response.css("body > div.rh_wrap.rh_wrap_stick_footer > section > div.rh_page.rh_page__map_properties > div.rh_page__listing > article> div > figure > a::attr(href)").extract()
        for url in urls:
            yield Request(url=url,
            callback = self.parse_property)
        next_page = ("https://medallioncorp.com/properties/page/"+ str(medallioncorp_com_PySpider_canadaSpider.page_number))
        if medallioncorp_com_PySpider_canadaSpider.page_number <= 9:
            medallioncorp_com_PySpider_canadaSpider.page_number += 1
            yield response.follow(next_page, callback=self.parse)

    def parse_property(self, response):
        units = response.css('body > div.rh_wrap.rh_wrap_stick_footer > section.rh_section.rh_wrap--padding.rh_wrap--topPadding > div > div > div > div.rh_property__main > div.rh_property__content.clearfix > div.rh_property__floor_plans.floor-plans > div > div').extract()
        counter = 1
        cc = 1
        for i in range(len(units)):
            item_loader = ListingLoader(response=response)
            rent = response.css('body > div.rh_wrap.rh_wrap_stick_footer > section.rh_section.rh_wrap--padding.rh_wrap--topPadding > div > div > div > div.rh_property__main > div.rh_property__content.clearfix > div.rh_property__floor_plans.floor-plans > div > div:nth-child('+str(cc)+') > div.floor-plan-title > div.floor-plan-meta > p.floor-price > span::text').get()
            room_count = None
            try:
                room_count = int(response.css('body > div.rh_wrap.rh_wrap_stick_footer > section.rh_section.rh_wrap--padding.rh_wrap--topPadding > div > div > div > div.rh_property__main > div.rh_property__content.clearfix > div.rh_property__floor_plans.floor-plans > div > div:nth-child('+str(cc)+') > div.floor-plan-title > div.floor-plan-meta > p::text').get().split(' ')[0])
            except:
                pass
            if room_count is None:
                room_count = int(response.css('body > div.rh_wrap.rh_wrap_stick_footer > section.rh_section.rh_wrap--padding.rh_wrap--topPadding > div > div > div > div.rh_property__main > div.rh_property__content.clearfix > div.rh_property__floor_plans.floor-plans > div > div:nth-child('+str(cc)+') > div.floor-plan-title > div.title > h3::text').get().split(' ')[0])
            cc = cc + 1

            if 'Starting From' in rent:
                rent = rent.replace('Starting From','')
            if ',' in rent:
                rent = int(rent.replace('$','').replace(',',''))
            else:
                rent = int(rent.replace('$',''))

            title = response.css('body > div.rh_wrap.rh_wrap_stick_footer > section.rh_section.rh_wrap--padding.rh_wrap--topPadding > div > div > div > div.rh_property__main > div.rh_page__head.rh_page__property > div.rh_page__property_title > h1::text').get()
            description = response.xpath("//meta[@name='description']/@content").get()
            address = response.css("body > div.rh_wrap.rh_wrap_stick_footer > section.rh_section.rh_wrap--padding.rh_wrap--topPadding > div > div > div > div.rh_property__main > div.rh_page__head.rh_page__property > div.rh_page__property_title > p::text").get()
            city = None
            try:
                city = address.split(', ')[1].split(',')[0]
            except:
                pass
            zipcode = None
            try:
                zipcode = address.split(',')[2].split(' ')[2]
            except:
                pass
            images = response.css('#property-detail-slider-two img::attr(src)').extract()
            external_images_count = len(images)

            body = response.css("body").get()
            info = ''
            all_info = response.css('.rh_content li::text').extract()
            for i in range(len(all_info)):
                info = info + ' ' +all_info[i]
            terrace = None
            parking = None
            washing_machine = None
            balcony = None
            if 'Balconies' in body:
                balcony = True
            if 'laundry' in body:
                washing_machine = True
            if 'Terrace' in body:
                terrace = True
            if 'Indoor Parking: $' in body:
                parking = True
            
            latlng = response.css("#property-google-map-js-extra::text").get()
            latitude = latlng.split('"lat":"')[1].split('"')[0]
            longitude = latlng.split('"lng":"')[1].split('"')[0]
            
            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            if zipcode is None:
                zipcode = responseGeocodeData['address']['Postal']
            if city is None:
                city = responseGeocodeData['address']['City']
            
            
            item_loader.add_value('external_link', response.url+f"#{counter}")        
            item_loader.add_value('external_source', self.external_source)
            item_loader.add_value('title',title)
            item_loader.add_value('description',description)
            item_loader.add_value('address',address)
            item_loader.add_value('city',city)
            item_loader.add_value('zipcode',zipcode)
            item_loader.add_value('latitude',latitude)
            item_loader.add_value('longitude',longitude)
            item_loader.add_value('property_type','apartment')
            item_loader.add_value('room_count',room_count)
            item_loader.add_value('rent',rent)
            item_loader.add_value('currency','CAD')
            item_loader.add_value('images',images)
            item_loader.add_value('external_images_count',external_images_count)
            item_loader.add_value('balcony',balcony)
            item_loader.add_value('terrace',terrace)
            item_loader.add_value('parking',parking)
            item_loader.add_value('washing_machine',washing_machine)
            item_loader.add_value('landlord_name','medallioncorp')
            item_loader.add_value('landlord_phone','(416) 256-3900')
            item_loader.add_value('landlord_email','info@medallioncorp.com')
            counter = counter + 1
            yield item_loader.load_item()
