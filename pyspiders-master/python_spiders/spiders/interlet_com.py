# -*- coding: utf-8 -*-
# Author: Pankaj Kalania
# Team: Sabertooth

import scrapy
from scrapy import Request
import re
from ..loaders import ListingLoader
from ..helper import extract_number_only, extract_rent_currency
from ..user_agents import random_user_agent
from geopy.geocoders import Nominatim


class InterletSpider(scrapy.Spider):
    name = 'interlet_com'
    allowed_domains = ['interlet.com']
    start_urls = ['http://interlet.com/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = [{
            'url': 'http://interlet.com/Search?listingType=6&statusids=2%2C6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&minprice=&maxprice=&bedrooms=&cipea=1&page=1'
            }
        ]

        for url in start_urls:
            yield Request(url=url.get('url'),
                          callback=self.parse,
                          meta={'page': 1,
                                'response_url': url.get('url')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[contains(@class,"searchListing")]/div')

        for listing in listings:
            url = listing.xpath('.//h2[@class="searchProName"]/a/@href').extract_first()
            beds = listing.xpath('.//h2[@class="searchProName"]/../div/div[contains(@class,"bed")]/text()').extract_first()
            baths = listing.xpath('.//h2[@class="searchProName"]/../div/div[contains(@class,"nath")]/text()').extract_first()
            url = response.urljoin(url)
            yield Request(url=url,
                          callback=self.get_property_details,
                          meta={'response_url': url,
                                'room_count': beds,
                                'bathroom_count': baths})
        # print(len(listings))
        if len(listings) > 0:
            next_page_url = re.sub(r"page=\d+", 'page='+str(response.meta.get('page')+1), response.meta.get('response_url'))
            # print(next_page_url)
            yield Request(url=next_page_url,
                          callback=self.parse,
                          meta={'response_url': next_page_url,
                                'property_type': response.meta.get('property_type'),
                                'page':response.meta.get('page')+1})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value('external_link', response.meta.get('response_url'))
        item_loader.add_value('external_link', response.meta.get('response_url').split('/')[-1])

        item_loader.add_xpath('title', './/div[contains(@class,"detailsItem")]//h2/text()')
        address = response.xpath('.//div[contains(@class,"detailsItem")]//h2/text()').extract_first()
        if address:
            item_loader.add_value('address', address)
            if len(extract_number_only(address.split(',')[-1]))==0 and len(address)>0:
                item_loader.add_value('city', address.split(',')[-1])
            elif len(address) > 1:
                item_loader.add_value('city', address.split(',')[-2])
                item_loader.add_value('zipcode', address.split(',')[-1])

        if address:
            geolocator = Nominatim(user_agent=random_user_agent())
            location = geolocator.geocode(query=address, addressdetails=True)
            if location:
                item_loader.add_value('latitude', str(location.latitude))
                item_loader.add_value('longitude', str(location.longitude))

        item_loader.add_xpath('description', './/div[contains(@class,"fdDescription")]/descendant-or-self::text()')
        rent_string = response.xpath('.//div[contains(@class,"detailsItem")]//h3/div/text()').extract_first()
        if "pw" in rent_string.lower():
            item_loader.add_value('rent_string', "£ " + str(extract_rent_currency(rent_string, InterletSpider)[0] * 4))
        else:
            item_loader.add_value('rent_string', rent_string)

        if response.meta.get("bathroom_count"):
            item_loader.add_value('bathroom_count', response.meta["bathroom_count"])
        else:
            bathroom_count = response.xpath('.//span[contains(text(), "Bathrooms")]/text()').extract_first()
            if bathroom_count:
                item_loader.add_value('bathroom_count', extract_number_only(bathroom_count))
        if response.meta.get('room_count'):
            if response.meta.get('room_count') == 0:
                item_loader.add_value('room_count', '1')
            else:
                item_loader.add_value('room_count', response.meta.get('room_count'))

        if any(item in item_loader.get_output_value('description').lower() for item in ['studio', 'bedsit']):
            item_loader.add_value('property_type', 'studio')
        elif any(item in item_loader.get_output_value('description').lower() for item in ['apartment']):
            item_loader.add_value('property_type', 'apartment')
        elif any(item in item_loader.get_output_value('description').lower() for item in ['house']):
            item_loader.add_value('property_type', 'house')
        else:
            return

        # http://interlet.com/property/residential/for-rent/london/knightsbridge/wilton-place/102111001716
        swimming_pool = response.xpath('.//div[contains(@class,"fdFeatures")]//li/span[contains(text(),"Swimming Pool")]/text()').extract_first()
        if swimming_pool:
            item_loader.add_value('swimming_pool', True)

        # http://interlet.com/property/residential/for-rent/london/knightsbridge/wilton-place/102111001716
        elevator = response.xpath('.//div[contains(@class,"fdFeatures")]//li/span[contains(text(),"Lift")]/text()').extract_first()
        if elevator:
            item_loader.add_value('elevator', True)

        # http://interlet.com/property/residential/for-rent/london/westminster/abbey-orchard-street/102111001843
        parking = response.xpath('.//div[contains(@class,"fdFeatures")]//li/span[contains(text(),"Parking") or contains(text(),"Garage")]/text()').extract_first()
        if parking:
            item_loader.add_value('parking', True)

        # http://interlet.com/property/residential/for-rent/london/kensington/lexham-gardens/102111001521
        washing_machine = response.xpath('.//div[contains(@class,"fdFeatures")]//li/span[contains(text(),"Washing Machine")]/text()').extract_first()
        if washing_machine:
            item_loader.add_value('washing_machine', True)

        # http://interlet.com/property/residential/for-rent/london/hammersmith/fulham-palace-road/102111001688
        furnished = response.xpath('.//div[contains(@class,"fdFeatures")]//li/span[contains(text(),"Fully Furnished")]/text()').extract_first()
        if furnished:
            item_loader.add_value('furnished', True)

        # http://interlet.com/property/residential/for-rent/london/shepherds-bush/loftus-road/102111000087
        dishwasher = response.xpath('.//div[contains(@class,"fdFeatures")]//li/span[contains(text(),"Dishwasher")]/text()').extract_first()
        if dishwasher:
            item_loader.add_value('dishwasher', True)

        # http://interlet.com/property/residential/for-rent/london/paddington/merchant-square-east/102111001904
        terrace = response.xpath('.//div[contains(@class,"fdFeatures")]//li/span[contains(text(),"Terrace")]/text()').extract_first()
        if terrace:
            item_loader.add_value('terrace', True)

        # http://interlet.com/property/residential/for-rent/london/westminster/abbey-orchard-street/102111001843
        balcony = response.xpath('.//div[contains(@class,"fdFeatures")]//li/span[contains(text(),"Balcony")]/text()').extract_first()
        if balcony:
            item_loader.add_value('balcony', True)

        item_loader.add_xpath('images', './/div[@id="property-photos-device2"]/a[@rel="propertyphotos"]/@href')
        item_loader.add_xpath('floor_plan_images', './/a[contains(text(),"Floor Plan")]/@href')

        # landlord information
        item_loader.add_value('landlord_phone', '+44 0 207 795 6525')
        item_loader.add_value('landlord_name', 'Interlet')

        self.position += 1
        item_loader.add_value('position', self.position )
        item_loader.add_value("external_source", "Interlet_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
