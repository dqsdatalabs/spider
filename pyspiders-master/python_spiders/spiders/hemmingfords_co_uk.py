# -*- coding: utf-8 -*-
# Author: Pankaj
# Team: Sabertooth

import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, extract_rent_currency
from datetime import date
import js2xml
import lxml.etree
from geopy.geocoders import Nominatim
from ..user_agents import random_user_agent
from scrapy import Selector


class HemmingfordsSpider(scrapy.Spider):
    name = "hemmingfords_co_uk"
    allowed_domains = ["www.hemmingfords.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):

        # start_urls = [{'url':'https://www.hemmingfords.co.uk/property-search?cat=10&location=&Types=Apartment&Bedrooms=&MinimumPrice=&MaximumPrice=&Distance=',
        #     'property_type':'apartment'},
        #     {'url':'https://www.hemmingfords.co.uk/property-search?cat=10&location=&Types=Penthouse&Bedrooms=&MinimumPrice=&MaximumPrice=&Distance=',
        #     'property_type':'apartment'},
        #     {'url':'https://www.hemmingfords.co.uk/property-search?cat=10&location=&Types=Studio&Bedrooms=&MinimumPrice=&MaximumPrice=&Distance=',
        #     'property_type':'studio'},
        #     ]

        # not taking urls per propertytype as many listings dont have any
        start_urls = [{'url': 'https://www.hemmingfords.co.uk/property-search?cat=10&location=&Types=&Bedrooms=&MinimumPrice=&MaximumPrice=&Distance='}]

        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'request_url': url.get('url'),
                                       # 'property_type':url.get('property_type')
                                       })

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@class="property-list"]/div')
        for listing in listings:
            property_url = listing.xpath(".//div/a/@href").extract_first()
            room_count = listing.xpath('.//i[contains(@class,"bed")]/../span/text()').extract_first()
            bathroom_count = listing.xpath('.//i[contains(@class,"bath")]/../span/text()').extract_first()

            yield scrapy.Request(
                url=property_url, 
                callback=self.get_property_details,
                meta={'request_url': property_url,
                      # 'property_type':response.meta.get('property_type'),
                      'room_count': room_count,
                      'bathroom_count': bathroom_count,
                      })

        next_page_url = response.xpath('.//a[contains(text(),">") and not(contains(text(),">>"))]/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse,
                meta={'request_url': next_page_url,
                      # 'property_type':response.meta.get('property_type')
                      })

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        if len(response.xpath('.//div[@class="pdStatusIcon"]//*[contains(text(), "Let Agreed")]')) > 0:
            return
        item_loader.add_value("external_link", response.meta.get('request_url'))
        item_loader.add_value("external_id", response.meta.get('request_url').split('/')[-1])

        item_loader.add_xpath('description', './/div[@id="propDetails"]/p/text()')

        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex"]
        house_types = ['chalet', 'bungalow', 'maison', 'house', 'home', 'villa']
        studio_types = ["studio"]
        if any(i in item_loader.get_output_value('description').lower() for i in studio_types):
            item_loader.add_value('property_type', 'studio')
        elif any(i in item_loader.get_output_value('description').lower() for i in apartment_types):
            item_loader.add_value('property_type', 'apartment')
        elif any(i in item_loader.get_output_value('description').lower() for i in house_types):
            item_loader.add_value('property_type', 'house')
        else:
            return

        if response.meta.get('room_count'):
            item_loader.add_value("room_count", response.meta.get('room_count'))
        else:
            if item_loader.get_output_value('property_type') == 'studio':
                item_loader.add_value("room_count", '1')

        if response.meta.get('bathroom_count'):
            item_loader.add_value("bathroom_count", response.meta.get('bathroom_count'))
        else:
            if item_loader.get_output_value('property_type') == 'studio':
                item_loader.add_value("bathroom_count", '1')
        
        # When no image is present, a \n is present instead
        floor_plan_images = response.xpath('.//img[@alt="Floorplan"]/@src').extract()
        for image in floor_plan_images:
            if len(image) > 10:
                item_loader.add_value("floor_plan_images", image)

        # may include floor plan images
        images = response.xpath('.//div[(@data-background) and contains(@data-background, "_original")]/@data-background').extract()
        if len(images) == 0:
            images = response.xpath('.//div[(@data-background) and contains(@data-background, "_large")]/@data-background').extract()
        item_loader.add_value("images", images)

        rent_string = response.xpath('.//div[contains(@class,"price")]/text()').extract_first()
        if rent_string and "pw" in rent_string.lower():
            rent_string = "£ " + str(extract_rent_currency(rent_string, HemmingfordsSpider)[0] * 4)
        item_loader.add_value('rent_string', rent_string)

        address = response.xpath('.//div[@class="swiper-slide"]//h3/text()').extract_first()
        item_loader.add_value('address', address)
        item_loader.add_value('city', address.split(",")[-2])
        item_loader.add_value('zipcode', address.split(",")[-1].strip().strip("."))

        title = response.xpath('.//title').extract_first()
        if title:
            item_loader.add_value('title', title.split(' | '))

        javascript = response.xpath('.//script[contains(text(),"var jslat") and contains(text(),"var jslng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)

            latitude = selector.xpath('.//var[@name="jslat"]/string/text()').extract_first()
            longitude = selector.xpath('.//var[@name="jslng"]/string/text()').extract_first()

            if latitude and longitude:
                item_loader.add_value('latitude', latitude)
                item_loader.add_value('longitude', longitude)

        features = ' '.join(response.xpath('.//div[@id="propFeatures"]//li/text()').extract())

        # https://www.hemmingfords.co.uk/to-rent/hemmingfords/london/1-bedroom-for-rent-in-freemasons-road-e16-3na/6261
        if ' furnished' in features.lower() and 'unfurnished' not in features.lower():
            item_loader.add_value('furnished', True)
        elif 'unfurnished' in features.lower() and ' furnished' not in features.lower():
            item_loader.add_value('furnished', False)

        if 'parking' in features.lower() or 'garage' in features.lower():
            item_loader.add_value('parking', True)

        if 'elevator' in features.lower() or 'lift' in features.lower():
            item_loader.add_value('elevator', True)
        
        if 'balcony' in features.lower():
            item_loader.add_value('balcony', True)

        if 'terrace' in features.lower():
            item_loader.add_value('terrace', True)

        if 'swimming pool' in features.lower():
            item_loader.add_value('swimming_pool', True)

        if 'washing machine' in features.lower():
            item_loader.add_value('washing_machine', True)

        if 'dishwasher' in features.lower():
            item_loader.add_value('dishwasher', True)

        item_loader.add_value('landlord_name', 'Hemmingfords')
        item_loader.add_value('landlord_email', 'info@hemmingfords.co.uk')
        item_loader.add_value('landlord_phone', '020 3890 7470')

        item_loader.add_value("external_source", "Hemmingfords_PySpider_{}_{}".format(self.country, self.locale))
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
