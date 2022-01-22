# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from scrapy.http import HtmlResponse
from geopy.geocoders import Nominatim
import requests
from ..user_agents import random_user_agent
import json
import js2xml
import re
import lxml.etree
from parsel import Selector
from ..helper import extract_number_only


class CapSud(scrapy.Spider):
    name = 'cap_sud_com'
    allowed_domains = ['cap-sud.com']
    start_urls = ['https://cap-sud.com/']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    external_source = 'CapSud_PySpider_belgium_fr'
    thousand_separator = '.'
    scale_separator = ','
    position = 0
    params = {
        'form_data[page]': '0',
        "form_data[preserve_page]": 'False',
        'form_data[job_category][]': "maison",
        'form_data[wpis_areas_total]': '16::2360',
        'form_data[wpis_areas_total_default]': '16::2360',
        'form_data[priceRent]': '85::7475',
        'form_data[priceRent_default]': '85::7475',
        'form_data[wpis_configuration_bedrooms]': '1::15',
        'form_data[wpis_configuration_bedrooms_default]': '1::15',
        'form_data[wpis_configuration_bathrooms_default]': '1::7',
        'form_data[search_location_lat]': 'False',
        'form_data[search_location_lng]': 'False',
        'form_data[proximity]': '5',
        'form_data[proximity_units]': 'km',
        'form_data[purpose-status-rent]': 'a-louer',
        'form_data[sort]': 'loyer-c',
        'listing_type': 'a-louer',
    }

    def start_requests(self):
        start_urls = [
            {
                'url': "https://cap-sud.com/?mylisting-ajax=1&action=get_listings&security=07b537e2f3",
                'job_category': 'maison',
                'property_type': 'house'
            },
            {
                # https://cap-sud.com/recherche/?type=a-louer&category%5B%5D=appartement&sort=loyer-c
                'url': "https://cap-sud.com/?mylisting-ajax=1&action=get_listings&security=07b537e2f3",
                'job_category': 'appartement',
                'property_type': 'apartment'
            }
        ]

        temp_url = "https://cap-sud.com/recherche/?type=a-louer&category%5B%5D=maison&sort=loyer-c"
        temp_response = requests.get(temp_url)
        temp_response = HtmlResponse(url=temp_url, body=temp_response.text, encoding='utf-8')
        javascript = temp_response.xpath('.//script[contains(text(), "ajax_nonce")]/text()').extract_first()
        xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
        selector = Selector(text=xml)
        security = selector.xpath('.//property[@name="ajax_nonce"]/string/text()').extract_first()

        for url in start_urls:
            api_url = re.sub(r"(?<=security=)\w+$", security, url.get("url"))
            self.params['form_data[job_category][]'] = url.get('job_category')
            yield scrapy.FormRequest(url=api_url,
                                     callback=self.parse,
                                     formdata=self.params,
                                     meta={
                                        'request_url': api_url,
                                        'params': self.params,
                                        'property_type': url.get('property_type')
                                     }
                                     )
    
    def parse(self, response, **kwargs):
        html_response = HtmlResponse(url="my HTML string", body=json.loads(response.text)['html'], encoding='utf-8')
        # listings = html_response.xpath('.//div[@data-id]')
        listings = html_response.xpath('.//div[contains(@data-id, "listing-id-")]')

        # zipcode = '//i[@class="mi location_on"]/../text()'
        # property_type = '//div[@class="lf-head"]/div[1]/text()'
        for listing in listings:
            url = listing.xpath('./div/a[contains(@href, "/list/")]/@href').extract_first()
            latitude = listing.xpath('./@data-latitude').extract_first()
            longitude = listing.xpath('./@data-longitude').extract_first()
            yield scrapy.Request(url=url,
                                 callback=self.get_property_details,
                                 meta={'request_url': url,
                                       'latitude': latitude,
                                       'longitude': longitude,
                                       'property_type': response.meta.get('property_type')}
                                 )

        if len(listings) > 0:
            response.meta.get('params')['form_data[page]'] = str(int(response.meta.get('params')['form_data[page]'])+1)
            yield scrapy.FormRequest(url=response.meta.get('request_url'),
                                     callback=self.parse,
                                     formdata=response.meta.get('params'),
                                     meta={
                                        'request_url': response.meta.get('request_url'),
                                        'property_type': response.meta.get('property_type'),
                                        'params': response.meta.get('params')
                                    }
                                 )
    
    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value('external_link', response.meta.get('request_url'))
        external_id = response.xpath('.//span[contains(text(),"ID : ")]/text()').extract_first()
        if external_id:
            item_loader.add_value('external_id', external_id.replace('ID : ', '').strip())
        square_meters = response.xpath('.//div[@class="container"]//span[contains(text(),"m2")]/text()').extract_first()
        if square_meters:
            item_loader.add_value('square_meters', square_meters.replace('m2', ''))
        room_count = response.xpath('//div[@class="container"]//span[contains(text(),"Chambre")]/text()').extract_first()
        if room_count:
            item_loader.add_value('room_count', extract_number_only(room_count))

        bathroom_count = response.xpath('.//div[@class="container"]//span[contains(text(),"Sdb")]/text()').extract_first()
        if bathroom_count:
            item_loader.add_xpath('bathroom_count', extract_number_only(bathroom_count))

        # rent_string = ''.join(response.xpath('.//div[@class="quick-listing-actions"]//div[contains(@class,"rent-price")]//text()').extract())
        rent_string = ''.join(response.xpath('.//i[contains(@class, "euro")]/../span/text()').extract())
        item_loader.add_value('rent_string', rent_string)
        # item_loader.add_value('rent',extract_number_only(rent_string))
    
        item_loader.add_xpath('city', './/i[@class="fa fa-map-signs"]/../span/text()')
        item_loader.add_xpath('address', "//a[i[@class='fa fa-map-signs']]/span/text()")

        # title1=response.xpath('//div[@class="pf-body"]/h1/div/text()').extract_first()
        # title2= response.xpath('//div[@class="pf-body"]/h1/text()').extract()
        # title = title1 + title2[0] + "À louer" + title2[-1]
        title = ' '.join(response.xpath('.//div[contains(@class,"body")]/h1//text()').extract())
        if title:
            item_loader.add_value('title', title)
        else:
            item_loader.add_xpath('title', '//title/text()')

        item_loader.add_xpath('images', './/div[contains(@class,"header-gallery-carousel")]//a/@href')
        item_loader.add_xpath('landlord_name', './/section[contains(@class,"profile-body")]//span[@class="host-name"]/text()')
        
        # landlord_phone = response.xpath('//i[contains(@class,"phone")]//../span/text()').get()
        # if landlord_phone:
        #     item_loader.add_value('landlord_phone', landlord_phone)
        # else:
        #     landlord_phone = response.xpath('//div[@class="pf-body"]//p[2]//text()').getall()
        #     if landlord_phone:
        #         item_loader.add_value('landlord_phone', landlord_phone[-1].strip())

        item_loader.add_value('landlord_email', "waterloo@cap-sud.com")
        item_loader.add_value('landlord_phone', '+32 2 387 42 42')
        #item_loader.add_xpath('landlord_email', './/i[contains(@class,"email")]//../span/text()')
        item_loader.add_xpath('description', '//div[@class="pf-body"]/p/text()')

        if response.meta.get('latitude'):
            item_loader.add_value('latitude', response.meta.get('latitude'))
        if response.meta.get('longitude'):
            item_loader.add_value('longitude', response.meta.get('longitude'))
        item_loader.add_value('property_type', response.meta.get('property_type'))

        """
        if response.meta.get('latitude') and response.meta.get('longitude'):
            geolocator = Nominatim(user_agent=random_user_agent())
            location = geolocator.reverse([response.meta.get('latitude'), response.meta.get('longitude')])
            if location:
                item_loader.add_value('address', location.address)
                if 'postcode' in location.raw['address']:
                    item_loader.add_value('zipcode', location.raw['address']['postcode'])
        """

        # https://cap-sud.com/list/4110468-jw-tilleul-24-charles-esther/
        elevator = response.xpath('.//span[contains(text(),"Ascenseur")]/text()').extract_first()
        if elevator:
            item_loader.add_value('elevator', True)

        # https://cap-sud.com/list/4110468-jw-tilleul-24-charles-esther/
        swimming_pool = response.xpath('.//span[contains(text(),"Piscine")]/text()').extract_first()
        if swimming_pool:
            item_loader.add_value('swimming_pool', True)

        energy_label = response.xpath('//span[contains(text(), "Espec:")]/text()').extract_first()
        if energy_label:
            item_loader.add_value('energy_label', energy_label.replace('Espec: ', ''))

        self.position += 1
        item_loader.add_value('position', self.position)
        
        return item_loader.load_item()

def decodeEmail(e):
    de = ""
    k = int(e[:2], 16)

    for i in range(2, len(e)-1, 2):
        de += chr(int(e[i:i+2], 16)^k)

    return de