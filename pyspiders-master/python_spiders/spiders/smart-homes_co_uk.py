# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
import scrapy
from ..helper import extract_number_only, format_date
from datetime import datetime
from ..loaders import ListingLoader
import json
import js2xml
import lxml


class SmartHomesCoUkSpider(scrapy.Spider):
    name = "smart_homes_co_uk"
    allowed_domains = ["smart-homes.co.uk"]
    start_urls = [
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=apartment&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "apartment"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=farm-house&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "house"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=shared-flat&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "apartment"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=barn-conversion&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "house"},
        # {"url":'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=commercial&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
        # "property_type":"Commercial"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=cottage&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "house"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=detached-bungalow&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "house"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=detached-house&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "house"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=end-terraced-house&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "house"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=flat&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "apartment"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=ground-floor-flat&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "apartment"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=ground-floor-maisonette&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "apartment"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=town-house&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "house"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=studio&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "studio"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=semi-detached-house&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "house"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=semi-detached-bungalow&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "house"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=penthouse&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "apartment"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=mid-terraced-house&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "house"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=mews&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "house"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=maisonette&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "apartment"},
        {"url": 'https://www.smart-homes.co.uk/search-properties/to-rent-solihull?keyword=&property-id=&location=any&status=2&ptype=link-detached&bedrooms=any&bathrooms=any&min-price=any&max-price=any',
         "property_type": "house"},
    ]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    external_source = 'SmartHomes_PySpider_united_kingdom_en'
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('//a[contains(@href,"/property/")]/@href').extract()
        listings = set(listings)
        for property_url in listings:
            property_url = response.urljoin(property_url)
            yield scrapy.Request(url=property_url,
                                 callback=self.get_property_details,
                                 meta={'request_url': property_url,
                                       'property_type': response.meta.get('property_type')}
                                 )
                                
        next_page_url = response.xpath('//a[@title="Next page"]/@href').get()
        if next_page_url:
            yield response.follow(
                url=next_page_url,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.meta.get("request_url"))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value('property_type', response.meta.get('property_type'))
        external_id = response.xpath('//h4[contains(text(),"Property ID")]/text()').extract_first()
        external_id = external_id.split(":")[1].strip()
        if external_id:
            item_loader.add_value('external_id', external_id)

        item_loader.add_xpath('title', '//title/text()')

        description = response.xpath('//h4[text()="Features"]/following-sibling::ul/li//text()').extract()
        description = ", ".join(description).strip()
        item_loader.add_value('description', description)

        address = response.xpath('//h4[@class="property-address"]/text()').extract_first()
        if address:
            address = address.split('In')[1].strip()
            item_loader.add_value('address', address)

            if "Lamprey" not in str(address):
                city = address.split(', ')[1]
                item_loader.add_value('city', city)
            else:
                item_loader.add_value("city","Birmingham")

        room_count_string = response.xpath('//span[contains(text(),"Bedroom")]/text()').extract_first()
        if room_count_string:
            room_count = extract_number_only(room_count_string)
            item_loader.add_value('room_count', room_count)
        
        bathroom_count_string = response.xpath('//span[contains(text(),"Bathroom")]/text()').extract_first()
        if bathroom_count_string:
            bathroom_count = extract_number_only(bathroom_count_string)
            item_loader.add_value('bathroom_count', bathroom_count)

        features = response.xpath('//h4[text()="Features"]/following-sibling::ul/li//text()').extract()
        if features:
            featuresString = " ".join(features)

            # https://www.smart-homes.co.uk/details/property/12267-bedroom-property-in-waterside-heights-dickens-heath            
            if "parking" or "garage" in featuresString.lower(): 
                item_loader.add_value('parking', True)

            # https://www.smart-homes.co.uk/details/property/12349-bedroom-property-in-waterside-heights-dickens-heath
            if "elevator" in featuresString.lower() or 'lift' in featuresString.lower(): 
                item_loader.add_value('elevator', True)

            # https://www.smart-homes.co.uk/details/property/12267-bedroom-property-in-waterside-heights-dickens-heath            
            if "balcony" in featuresString.lower(): 
                item_loader.add_value('balcony', True)

            if "terrace" in featuresString.lower(): 
                item_loader.add_value('terrace', True)

            if "swimming pool" in featuresString.lower():
                item_loader.add_value('swimming_pool', True)

            if "washing machine" in featuresString.lower():
                item_loader.add_value('washing_machine', True)

            if "dishwasher" in featuresString.lower() or "washer" in featuresString.lower():
                item_loader.add_value('dishwasher', True)
    
            # https://www.smart-homes.co.uk/details/property/17969-bedroom-property-in-arnold-road-shirley
            if " furnished" in featuresString.lower(): 
                item_loader.add_value('furnished', True)

            # https://www.smart-homes.co.uk/details/property/12349-bedroom-property-in-waterside-heights-dickens-heath
            elif "unfurnished" in featuresString.lower() or "un-furnished" in featuresString.lower(): 
                item_loader.add_value('furnished', False)

            # https://www.smart-homes.co.uk/details/property/24902-bedroom-property-in-thornby-avenue-solihull
            if "pets considered" in featuresString.lower(): 
                item_loader.add_value('pets_allowed', True)

        item_loader.add_xpath('rent_string', './/span[contains(text(),"£")]/text()')
        # https://www.smart-homes.co.uk/details/property/25090-bedroom-property-in-wadbarn-dickens-heath # no longer valid
        available_date_string = response.xpath('//li[contains(text(),"Available")]/text()').extract_first()
        if available_date_string:
            available_date_string = available_date_string.split()[1]
            available_date = format_date(available_date_string, '%d/%m/%Y')
            if available_date_string != available_date:
                item_loader.add_value('available_date', available_date)

        images = [x for x in response.xpath("//ul[@class='slides']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
    
        item_loader.add_value('landlord_name', "JHS Ltd T/As Smart Homes.")
        item_loader.add_value('landlord_phone', '0121 745 9777')

        latlng_script = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if latlng_script:
            lat = latlng_script.split('latitude":')[1].split(",")[0]
            lng = latlng_script.split('longitude":')[1].split("},")[0]
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)


        yield item_loader.load_item()

    

