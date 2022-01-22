# -*- coding: utf-8 -*-
# Author: Pankaj Kalania
# Team: Sabertooth

import scrapy
from geopy.geocoders import Nominatim
import re
from ..loaders import ListingLoader
from ..user_agents import random_user_agent


class GascoignehalmanSpider(scrapy.Spider):
    name = 'gascoignehalman_co_uk'
    allowed_domains = ['gascoignehalman.co.uk']
    start_urls = ['https://www.gascoignehalman.co.uk']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = [
            {
                'url': 'https://www.gascoignehalman.co.uk/search/?instruction_type=Letting&showstc=on&address_keyword_fields=town%2Cpostcode&address_keyword_exact=1&address_keyword=&remote_property_type=Barn+Conversion%2CBuilding+Plot%2CCottage%2CFarmhouse%2CHall%2CHouse%2CLodge%2CTown+House#lettings',
                'property_type': 'house'
            },
            {
                        
                'url': 'https://www.gascoignehalman.co.uk/search/?instruction_type=Letting&showstc=on&address_keyword_fields=town%2Cpostcode&address_keyword_exact=1&address_keyword=&remote_property_type=Flat%2CMaisonette%2CPenthouse%2CRetirement+Apartment%2CStudio%2CAPAR',
                'property_type': 'apartment'
            },
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'request_url': url.get('url'),
                                       'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[contains(@class,"property")]')
        for property_item in listings:
            property_url = response.urljoin(
                property_item.xpath('.//div[contains(@class,"nopadding")]/a/@href').extract_first())
            room = property_item.xpath('.//img[contains(@src,"bed")]/following-sibling::text()').extract_first()
            bathroom = property_item.xpath('.//img[contains(@src,"bath")]/following-sibling::text()').extract_first()
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'request_url': property_url,
                      'property_type': response.meta.get('property_type'),
                      'room': room,
                      'bathroom': bathroom}
            )

        next_page_url = response.xpath('.//a[contains(text(),"Next»")]/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'property_type': response.meta.get('property_type')}
            )

    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)

        external_link = response.meta.get('request_url').split("?")[0]
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_link.split('property/')[1].split("?")[0])
    
        item_loader.add_value('property_type', response.meta.get('property_type'))
        
        item_loader.add_xpath('title', './/aside[contains(@class,"links")]//h1/text()')
        item_loader.add_xpath('address', './/aside[contains(@class,"links")]//h1/text()')

        if item_loader.get_output_value('address'):
            geolocator = Nominatim(user_agent=random_user_agent())
            location = geolocator.geocode(item_loader.get_output_value('address'))
            if location and 'address' in location.raw:
                if 'postcode' in location.raw['address']:
                    item_loader.add_value('zipcode', location.raw['address']['postcode'])
                if 'city' in location.raw['address']:
                    item_loader.add_value('city', location.raw['address']['city'])
                if location.latitude:
                    item_loader.add_value('latitude', location.latitude)
                if location.longitude:
                    item_loader.add_value('longitude', location.longitude)
            else:
                item_loader.add_value('city', item_loader.get_output_value('address').split(',')[-1])

        # rent_string = response.xpath('.//aside[contains(@class,"links")]//h2/text()').extract_first().split('.')[0]
        item_loader.add_xpath('rent_string', './/aside[contains(@class,"links")]//h2/text()')
        item_loader.add_xpath('description', './/aside[contains(@class,"links")]/div/p/text()')
        description = response.xpath('.//aside[contains(@class,"links")]/div/p/text()').extract_first()
        if description and len(re.findall(r"(?<=energy efficiency rating)[^\w]*[a-z][']", description.lower())) > 0:
            energy_label = re.findall(r"(?<=energy efficiency rating)[^\w]*[a-z][']", description.lower())[0]
            item_loader.add_value('energy_label', re.sub(r"[^a-z]*", "", energy_label))
        item_loader.add_xpath('images', './/div[@class="item"]//img/@src')
        if response.meta.get('bathroom'):
            item_loader.add_value('bathroom_count', response.meta.get('bathroom'))
        if response.meta.get('room'):
            item_loader.add_value('room_count', response.meta.get('room'))
        item_loader.add_value('landlord_name', 'Gascoigne Halman')
        item_loader.add_value('landlord_phone', '01625 506 720')

        floor_plan_images = [response.urljoin(url).split("?")[0] for url in response.xpath('.//a[contains(text(),"View Floorplans")]/@href').extract()]
        item_loader.add_value('floor_plan_images', floor_plan_images)

        # ex https://www.gascoignehalman.co.uk/property/702657
        if "parking" in item_loader.get_output_value('description').lower() or "garage" in item_loader.get_output_value('description').lower():
            item_loader.add_value('parking', True)
        # set because of
        # ex https://www.gascoignehalman.co.uk/property/702657
        if "balcony" in item_loader.get_output_value('description').lower():
            item_loader.add_value('balcony', True)
        # ex https://www.gascoignehalman.co.uk/property/702657
        if "terrace" in item_loader.get_output_value('description').lower():
            item_loader.add_value('terrace', True)
        # ex 
        if "fully furnished" in item_loader.get_output_value('description').lower():
            item_loader.add_value('furnished', True)

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Gascoignehalman_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
