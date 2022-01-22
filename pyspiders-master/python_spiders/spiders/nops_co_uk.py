# -*- coding: utf-8 -*-
# Author: Gabriel Francis
# Team: Sabertooth

import js2xml
import lxml
import scrapy
from ..helper import extract_number_only
from scrapy import Selector

from ..loaders import ListingLoader


class NopsSpider(scrapy.Spider):
    name = 'nops_co_uk'
    allowed_domains = ['nops.co.uk']
    start_urls = ['http://nops.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = ['https://nops.co.uk/let/property-to-let/']
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse)

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[contains(@id,"property")]')
        for listing in listings:
            listing_url = response.urljoin(listing.xpath('.//h2[@class="title"]/a[contains(@href,"property")]/@href').extract_first())
            bed_bath_count = listing.xpath('.//div[@class="prop_item"]/text()').extract()
            listing_bedroom_count = None
            listing_bathroom_count = None
            for item in bed_bath_count:
                if 'bed' in item.lower():
                    listing_bedroom_count = extract_number_only(item)
                elif 'bath' in item.lower():
                    listing_bathroom_count = extract_number_only(item)

            currency = listing.xpath('.//span[@class="nativecurrencysymbol"]/text()').extract_first()
            rent = listing.xpath('.//span[@class="nativecurrencyvalue"]/text()').extract_first()
            yield scrapy.Request(
                url=listing_url,
                callback=self.get_property_details,
                meta={'request_url': listing_url,
                      'rent_string': currency + rent if currency and rent else None,
                      'room_count': listing_bedroom_count,
                      'bathroom_count': listing_bathroom_count})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.meta.get('request_url'))
        item_loader.add_value("external_id", response.meta.get('request_url').split('/')[4])
        item_loader.add_value("external_source", "Nops_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value("rent_string", response.meta.get('rent_string'))
        item_loader.add_value("room_count", response.meta.get('room_count'))
        item_loader.add_value("bathroom_count", response.meta.get('bathroom_count'))
        item_loader.add_xpath("title", './/h2[@class="info_title"]/text()')
        item_loader.add_xpath("description", './/div[@class="description_wrapper"]/text()')
        item_loader.add_xpath("images", './/div[contains(@class,"imageviewer")]//div[contains(@data-image-src,"jpg")]/@data-image-src')
        item_loader.add_xpath("floor_plan_images", './/h2[contains(text(),"Floorplan")]/..//a/img/@src')
        item_loader.add_xpath("address", './/div[@class="info_address"]/text()')

        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex"]
        house_types = ['chalet', 'bungalow', 'maison', 'house', 'home', 'villa']
        studio_types = ["studio"]

        if any(i in ' '.join(item_loader.get_output_value('description').split('.')[:3]).lower() for i in studio_types):
            item_loader.add_value('property_type', 'studio')
        elif any(i in ' '.join(item_loader.get_output_value('description').split('.')[:3]).lower() for i in apartment_types):
            item_loader.add_value('property_type', 'apartment')
        elif any(i in ' '.join(item_loader.get_output_value('description').split('.')[:3]).lower() for i in house_types):
            item_loader.add_value('property_type', 'house')
        else:
            return

        javascript = response.xpath('.//script[contains(text(),"latitude")]/text()').extract_first()
        location = None
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)

            # opposite values are taken on purpose. it is an error with the source
            latitude = xml_selector.xpath('.//var[@name="longitude"]/string/text()').extract_first()
            longitude = xml_selector.xpath('.//var[@name="latitude"]/string/text()').extract_first()

            if latitude and longitude:
                item_loader.add_value('latitude', latitude)
                item_loader.add_value('longitude', longitude)

        if not location:
            item_loader.add_value('city', item_loader.get_output_value('address').split(', ')[-2])
            item_loader.add_value('zipcode', item_loader.get_output_value('address').split(', ')[-1])

        # https://nops.co.uk/property/30071277/ox4/oxford/between-towns-road/flat/1-bedroom
        parking = response.xpath('.//div[@class="feature" and contains(text(),"Parking")]/text()').extract_first()
        if parking:
            if 'no' not in parking.lower() and '0' not in parking.lower():
                item_loader.add_value('parking',True)
            # https://nops.co.uk/property/30057503/ox4/oxford/jeune-street/flat/1-bedroom
            else:
                item_loader.add_value('parking',False)

        # https://nops.co.uk/property/30071277/ox4/oxford/between-towns-road/flat/1-bedroom
        furnished = response.xpath('.//div[@class="feature" and contains(text(),"Furnished")]/text()').extract_first()
        if furnished and 'not' not in furnished.lower():
            item_loader.add_value('furnished',True)

        # https://nops.co.uk/property/30142910/ox4/oxford/east-avenue/flat/2-bedrooms
        pets_allowed = response.xpath('.//div[@class="feature" and contains(text(),"Pets Allowed")]/text()').extract_first()
        if pets_allowed:
            if 'no' not in pets_allowed.lower():
                item_loader.add_value('pets_allowed',True)
            else:
                item_loader.add_value('pets_allowed',False)

        # washing_machine not present in features
        # dishwasher not present in features
        # balcony not present in features
        # terrace not present in features
        # elevator not present in features

        item_loader.add_value('landlord_name', 'NOPS Property Letting')
        item_loader.add_value('landlord_email', 'post@nops.co.uk')
        item_loader.add_value('landlord_phone', '01865 318538')

        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
