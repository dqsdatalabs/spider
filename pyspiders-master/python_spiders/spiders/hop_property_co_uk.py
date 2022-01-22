# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from geopy import Nominatim

from ..helper import extract_number_only
from ..loaders import ListingLoader
from ..user_agents import random_user_agent


class HopPropertySpider(scrapy.Spider):
    name = 'hop_property_co_uk'
    allowed_domains = ['hop-property.co.uk']
    start_urls = ['https://www.hop-property.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.hop-property.co.uk/property-search/search-results/?action=search&type=property&tenure=lettings&page=0&sort=price-highest&per-page=12&view=list&housing=student&price-min=0&price-max=999999999999&bedrooms-min=0&bedrooms-max=999999&property-type%5B%5D=Studio&include=true",
                "property_type": "studio"
            },
            {
                "url": "https://www.hop-property.co.uk/property-search/search-results/?action=search&type=property&tenure=lettings&page=0&sort=price-highest&per-page=100&view=list&housing=professional&price-min=0&price-max=999999999999&bedrooms-min=0&bedrooms-max=999999&property-type%5B%5D=Apartment&include=true",
                "property_type": "apartment"
            },
            {
                'url': 'https://www.hop-property.co.uk/property-search/search-results/?action=search&type=property&tenure=lettings&page=0&sort=price-highest&per-page=12&view=list&housing=professional&price-min=0&price-max=999999999999&bedrooms-min=0&bedrooms-max=999999&property-type%5B%5D=House+Share&include=true',
                'property_type': 'house'},
            {
                'url': 'https://www.hop-property.co.uk/property-search/search-results/?action=search&type=property&tenure=lettings&page=0&sort=price-highest&per-page=50&view=list&housing=student&price-min=0&price-max=999999999999&bedrooms-min=0&bedrooms-max=999999&property-type%5B%5D=Apartment&include=true',
                'property_type': 'apartment'}
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'request_url': url.get('url'),
                                       'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//a[@class="link-cover"]/@href').getall()
        for property_item in listings:
            yield scrapy.Request(
                url=property_item,
                callback=self.get_property_details,
                meta={'request_url': property_item,
                      'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_xpath('external_id', './/input[@id="book-viewing-property"]/@value')
        item_loader.add_xpath('title', './/div[contains(@class, "property-details")]//p[@class="lead"]/text()')
        item_loader.add_xpath('rent_string',
                              './/div[contains(@class, "property-details")]//p[contains(text(), "per month")]/..//b/text()')
        item_loader.add_xpath('description', './/h2[contains(text(),"description")]/parent::div/p/text()')

        item_loader.add_xpath('images', './/div[@id="property-slider"]//img/@data-src')
        room_count = response.xpath(
            './/div[contains(@class, "property-details")]//span[contains(@class, "bed")]/following-sibling::text()').extract_first()
        if room_count:
            item_loader.add_value('room_count', extract_number_only(room_count))
        bathroom = response.xpath(
            './/div[contains(@class, "property-details")]//span[contains(@class, "bath")]/following-sibling::text()').extract_first()
        if bathroom:
            item_loader.add_value('bathroom_count', extract_number_only(bathroom))
        else:
            bathroom = response.xpath("//li[contains(.,'BATHROOM') or contains(.,'Bathroom')]/text()").get()
            if bathroom and bathroom.split(" ")[0].isdigit():
                item_loader.add_value("bathroom_count", bathroom.split(" ")[0])
                
        item_loader.add_value('landlord_name', 'hop-property')
        phone = response.xpath('.//ul[@class="list-inline"]//a[contains(@href,"tel")]/@href').extract_first().split(":")[-1]
        item_loader.add_value('landlord_phone', phone)
        item_loader.add_value('landlord_email', 'contact@hop-property.co.uk')
        item_loader.add_xpath('floor_plan_images',
                              './/div[@id="property-tabs-floorplans"]//img[contains(@alt,"EPC")]/@data-src')

        latitude = response.xpath('.//div[@class="marker"]/@data-lat').extract_first()
        longitude = response.xpath('.//div[@class="marker"]/@data-lng').extract_first()
        if latitude and longitude:
            item_loader.add_value('latitude', latitude)
            item_loader.add_value('longitude', longitude)
            # geolocator = Nominatim(user_agent=random_user_agent())
            # location = geolocator.reverse(f"{latitude}, {longitude}")
            # if location:
            #     item_loader.add_value('address', location.address)
            #     if 'address' in location.raw:
            #         if 'postcode' in location.raw['address']:
            #             item_loader.add_value('zipcode', location.raw['address']['postcode'])
            #         if 'city' in location.raw['address']:
            #             item_loader.add_value('city', location.raw['address']['city'])
        
        address = response.xpath('.//div[@class="property-details__inner"]/p[@class="lead"]/text()').extract_first()
        if address:
            item_loader.add_value('address', address)
            item_loader.add_value('city', address.split(",")[-1].strip())

        # ex https://www.hop-property.co.uk/property/west-point-city-centre/
        parking = response.xpath('.//ul[contains(@class, "list-bullet")]/li[contains(text(), "Parking")]/text()')
        if parking:
            item_loader.add_value('parking', True)

        # ex https://www.hop-property.co.uk/property/west-point-city-centre/
        balcony = response.xpath('.//ul[contains(@class, "list-bullet")]/li[contains(text(), "Balcony")]/text()')
        if balcony:
            item_loader.add_value('balcony', True)

        # ex https://www.hop-property.co.uk/property/all-bills-included-the-pavilion-headingley/
        terrace = response.xpath('.//ul[contains(@class, "list-bullet")]/li[contains(text(), "TERRACE")]/text()')
        if terrace:
            item_loader.add_value('terrace', True)

        # ex https://www.hop-property.co.uk/property/all-bills-included-the-pavilion-headingley/
        furnished_p_tag = response.xpath('.//p[contains(text(),"Furnished")]')
        furnished_li_tag = response.xpath(
            './/ul[contains(@class, "list-bullet")]/li[contains(text(), "FURNISHED")]/text()')
        if furnished_li_tag or furnished_p_tag:
            item_loader.add_value('furnished', True)

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "HopProperty_PySpider_{}_{}".format(self.country, self.locale))
        status = response.xpath("//p[contains(@class,'slider__status')]//text()").get()
        if status:
            return
        else:
            yield item_loader.load_item()
