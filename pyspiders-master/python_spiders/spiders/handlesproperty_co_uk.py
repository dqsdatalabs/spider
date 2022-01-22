# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader
from ..helper import extract_number_only
import urllib.parse as urlparse
from urllib.parse import parse_qs
from scrapy import Selector
import requests


class HandlespropertySpider(scrapy.Spider):
    name = "handlesproperty_co_uk"
    allowed_domains = ["handlesproperty.co.uk"]
    start_urls = [
        {'url': 'https://www.handlesproperty.co.uk/?id=8130&do=search&for=2&cats=1&type%5B%5D=6&minprice=0&maxprice=99999999999&kwa%5B%5D=&minbeds=0&order=2&page=0&do=search', 'property_type': 'House'},
        {'url': 'https://www.handlesproperty.co.uk/?id=8130&do=search&for=2&cats=1&type%5B%5D=7&minprice=0&maxprice=99999999999&kwa%5B%5D=&minbeds=0&order=2&page=0&do=search', 'property_type': 'Penthouse'},
        {'url': 'https://www.handlesproperty.co.uk/?id=8130&do=search&for=2&cats=1&type%5B%5D=8&minprice=0&maxprice=99999999999&kwa%5B%5D=&minbeds=0&id=8130&order=2&page=0&do=search', 'property_type': 'Apartment'},
        {'url': 'https://www.handlesproperty.co.uk/?id=8130&do=search&for=2&cats=1&type%5B%5D=9&minprice=0&maxprice=99999999999&kwa%5B%5D=&minbeds=0&order=2&page=0&do=search', 'property_type': 'Cottage'},
        {'url': 'https://www.handlesproperty.co.uk/?id=8130&do=search&for=2&cats=1&type%5B%5D=10&minprice=0&maxprice=99999999999&kwa%5B%5D=&minbeds=0&id=8130&order=2&page=0&do=search,', 'property_type': 'Villa'},
        {'url': 'https://www.handlesproperty.co.uk/?id=8130&do=search&for=2&cats=1&type%5B%5D=11&minprice=0&maxprice=99999999999&kwa%5B%5D=&minbeds=0&id=8130&order=2&page=0&do=search', 'property_type': 'Maisonette'},
        {'url': 'https://www.handlesproperty.co.uk/?id=8130&do=search&for=2&cats=1&type%5B%5D=12&minprice=0&maxprice=99999999999&kwa%5B%5D=&minbeds=0&id=8130&order=2&page=0&do=search', 'property_type': 'Warehouse Conversion'},
        {'url': 'https://www.handlesproperty.co.uk/?id=8130&do=search&for=2&cats=1&type%5B%5D=13&minprice=0&maxprice=99999999999&kwa%5B%5D=&minbeds=0&id=8130&order=2&page=0&do=search', 'property_type': 'Flat'},
        {'url': 'https://www.handlesproperty.co.uk/?id=8130&do=search&for=2&cats=1&type%5B%5D=14&minprice=0&maxprice=99999999999&kwa%5B%5D=&minbeds=0&order=2&page=0&do=search', 'property_type': 'Bungalow'},
        {'url': 'https://www.handlesproperty.co.uk/?id=8130&do=search&for=2&cats=1&type%5B%5D=15&minprice=0&maxprice=99999999999&kwa%5B%5D=&minbeds=0&id=8130&order=2&page=0&do=search', 'property_type': 'End of Terrace'},
        {'url': 'https://www.handlesproperty.co.uk/?id=8130&do=search&for=2&cats=1&type%5B%5D=16&minprice=0&maxprice=99999999999&kwa%5B%5D=&minbeds=0&order=2&page=0&do=search', 'property_type': 'Semi-detached House'},
        {'url': 'https://www.handlesproperty.co.uk/?id=8130&do=search&for=2&cats=1&type%5B%5D=17&minprice=0&maxprice=99999999999&kwa%5B%5D=&minbeds=0&order=2&page=0&do=search', 'property_type': 'Detached House'},
        {'url': 'https://www.handlesproperty.co.uk/?id=8130&do=search&for=2&cats=1&type%5B%5D=18&minprice=0&maxprice=99999999999&kwa%5B%5D=&minbeds=0&id=8130&order=2&page=0&do=search', 'property_type': 'Studio'},
        {'url': 'https://www.handlesproperty.co.uk/?id=8130&do=search&for=2&cats=1&type%5B%5D=23&minprice=0&maxprice=99999999999&kwa%5B%5D=&minbeds=0&order=2&page=0&do=search', 'property_type': 'Residential Student Property'},
        {'url': 'https://www.handlesproperty.co.uk/?id=11222&do=search&for=2&cats=2&type%5B%5D=&minprice=0&maxprice=99999999999&kwa%5B%5D=&minbeds=0&order=2&page=0&do=search', 'property_type': 'Student Property'}
    ]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        all_page_urls = response.xpath('.//div[@class="results-pagination-wrap"]//a/@href').extract()
        for url in all_page_urls:
            url = response.urljoin(url)
            yield scrapy.Request(url=url,
                                 callback=self.parse_listings_page,
                                 meta={'property_type': response.meta.get('property_type')})

    def parse_listings_page(self, response):
        listings = response.xpath('.//a[text()="View Details"]/@href').extract()
        for property_url in listings:
            property_url = response.urljoin(property_url)
            yield scrapy.Request(url=property_url,
                                 callback=self.get_property_details,
                                 meta={'request_url': property_url,
                                       'property_type': response.meta.get('property_type')}
                                 )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        # property_type
        property_mapping = {'House': 'house',
                            'Penthouse': 'apartment',
                            'Apartment': 'apartment',
                            'Cottage': 'house',
                            'Villa': 'house',
                            'Maisonette': 'apartment',
                            'Warehouse Conversion': 'apartment',
                            'Flat': 'apartment',
                            'Bungalow': 'house',
                            'End of Terrace': 'house',
                            'Semi-detached House': 'house',
                            'Detached House': 'house',
                            'Studio': 'studio',
                            'Residential Student Property': 'house',
                            'Student Property': 'student_apartment'}
        property_type = response.meta.get('property_type')
        for key_i in property_mapping:
            property_type = property_type.replace(key_i, property_mapping[key_i])
        item_loader.add_value('property_type', property_type)

        external_id = response.meta.get('request_url').split('pid=')[-1]
        item_loader.add_value('external_id', external_id)

        item_loader.add_xpath('title', './/h2[@class="details-address1"]/text()')
        item_loader.add_xpath('description', './/div[@class="details-description"]//text()')
        room_count = response.xpath('.//title/text()').extract_first()
        if room_count and len(re.findall(r"\d+[^\w]*bed", room_count.lower())) > 0:
            room_count = re.findall(r"\d+[^\w]*bed", room_count.lower())[0]
            item_loader.add_value('room_count', extract_number_only(room_count))
        elif "studio" in item_loader.external_source_out("description").lower():
            item_loader.add_value('room_count', "1")

        coordinatesUrl = response.xpath('.//div[@id="maps"]/iframe/@src').extract_first()
        if coordinatesUrl:
            parsed = urlparse.urlparse(coordinatesUrl)
            coordinates = parse_qs(parsed.query)['ll'][0].split(',')
            if coordinates:
                latitude = coordinates[0]
                longitude = coordinates[1]
                item_loader.add_value('latitude', latitude)
                item_loader.add_value('longitude', longitude)

        bathroom = "".join(response.xpath("//div[@class='details-features features-list']//text()[contains(.,'Bathrooms')]").extract())
        if bathroom:
            from word2number import w2n
            bath = bathroom.split("Bathrooms")[0].strip().split(" ")[-1].strip()        
            number = w2n.word_to_num(bath)
            item_loader.add_value('bathroom_count', number)

        address = response.xpath('.//h2[@class="details-address1"]/text()').extract_first()
        item_loader.add_value('address', address)
        city = address.split(',')[-2].strip()
        zipcode = address.split(',')[-1].strip()
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)

        features = response.xpath('.//div[@class="details-features features-list"]//text()').extract()
        featuresString = " ".join(features)
        # https://www.handlesproperty.co.uk/property-search~action=detail,pid=196
        if "parking" in featuresString.lower():
            item_loader.add_value('parking', True)

        if "elevator" in featuresString.lower() or 'lift' in featuresString.lower(): 
            item_loader.add_value('elevator', True)

        # https://www.handlesproperty.co.uk/property-search~action=detail,pid=569
        if "balcony" in featuresString.lower():
            item_loader.add_value('balcony', True)

        if "terrace" in featuresString.lower():
            item_loader.add_value('terrace', True)

        if "swimming pool" in featuresString.lower():
            item_loader.add_value('swimming_pool', True)

        if "washing machine" in featuresString.lower():
            item_loader.add_value('washing_machine', True)

        if "dishwasher" in featuresString.lower():
            item_loader.add_value('dishwasher', True)

        # https://www.handlesproperty.co.uk/property-search~action=detail,pid=526
        if "furnished" in featuresString.lower():
            if len(re.findall(r"un[^\w]*furnished", featuresString.lower())) > 0:
                item_loader.add_value('furnished', False)
            else:
                item_loader.add_value('furnished', True)
        """
        if " furnished" in featuresString.lower() and "unfurnished" not in featuresString.lower():
            item_loader.add_value('furnished', True)
        # https://www.handlesproperty.co.uk/property-search~action=detail,pid=357
        elif ("unfurnished" in featuresString.lower() or "un-furnished" in featuresString.lower()) and " furnished" not in featuresString.lower():
            item_loader.add_value('furnished', False)
        """

        item_loader.add_xpath("rent_string", './/h4[@class="detail-price"]/text()')
        item_loader.add_xpath('images', '//div[@id="galleria"]//a/@href')
        floorPlanIframeUrl = response.xpath('//div[@id="floorplan"]//iframe/@src').extract_first()
        if floorPlanIframeUrl:
            floorPlanIframeUrl = response.urljoin(floorPlanIframeUrl.lstrip(' /'))
            item_loader.add_value('floor_plan_images', Selector(text=requests.get(floorPlanIframeUrl).text).xpath('.//img/@src').extract_first())

        item_loader.add_value("external_link", response.meta.get('request_url'))
        item_loader.add_value('landlord_name', "Handles Property")
        item_loader.add_value('landlord_email', "leamington@handlesproperty.co.uk")
        item_loader.add_value('landlord_phone', "01926 354 400")
        item_loader.add_value("external_source", "Handlesproperty_PySpider_{}_{}".format(self.country, self.locale))

        self.position += 1
        item_loader.add_value("position", self.position)
        yield item_loader.load_item()
