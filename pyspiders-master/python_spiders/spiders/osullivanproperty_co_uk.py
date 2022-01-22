# -*- coding: utf-8 -*-
# Author: Karan Katle
# Team: Sabertooth

import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, remove_white_spaces, extract_rent_currency, format_date
from geopy.geocoders import Nominatim
import re
from datetime import date
from ..user_agents import random_user_agent


class OsullivanpropertySpider(scrapy.Spider):
    name = "osullivanproperty_co_uk"
    allowed_domains = ["osullivanproperty.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    listing_new = []
    position = 0

    def start_requests(self):
        start_urls = [
            {'url': 'https://osullivanproperty.co.uk/property-to-rent/flat/any-bed/all-location?page=1',
                'property_type': 'apartment'},
            {'url': 'https://osullivanproperty.co.uk/property-to-rent/bungalow/any-bed/all-location?page=1',
                'property_type': 'house'},
            {'url': 'https://osullivanproperty.co.uk/property-to-rent/house/any-bed/all-location?page=1',
                'property_type': 'house'},
            {'url': 'https://osullivanproperty.co.uk/property-to-rent/apartment/any-bed/all-location?page=1',
                'property_type': 'apartment'},
            {'url': 'https://osullivanproperty.co.uk/property-to-rent/residential-development/any-bed/all-location?page=1',
                'property_type': 'residential-development'}
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'request_url': url.get('url'),
                                       'property_type': url.get('property_type')}
                                 )

    def parse(self, response, **kwargs):
        listings = response.xpath('.//a[@class="card-image-container"]')
        for listing in listings:
            property_url = listing.xpath('.//@href').extract_first()
            if property_url not in self.listing_new:
                self.listing_new.append(property_url)
                yield scrapy.Request(
                    url=response.urljoin(property_url),
                    callback=self.get_property_details,
                    meta={'request_url': response.urljoin(property_url),
                          "property_type": response.meta["property_type"],
                          'title': listing.xpath('.//img/@alt').extract_first()}
                )

        if len(response.xpath('.//a[@class="card-image-container"]')) > 0:
            current_page = re.findall(r"(?<=page=)\d+", response.meta["request_url"])[0]
            next_page_url = re.sub(r"(?<=page=)\d+", str(int(current_page) + 1), response.meta["request_url"])
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'property_type': response.meta["property_type"]}
                )
            
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value('external_link', response.meta.get('request_url'))
        title = response.xpath('.//*[@class="header span12"]/h1//text()').extract()
        titles_join = ''.join(title)
        item_loader.add_value('title', titles_join)

        external_id = response.xpath('.//*[contains(text(),"Reference")]/text()').extract_first()
        item_loader.add_value('external_id', remove_white_spaces(external_id.split(':')[-1]))
        item_loader.add_xpath('description', './/*[@class="desc"]/text()')
        description = item_loader.get_output_value('description').lower()

        studio_types = ["studio", "studio apartment", "studio flat"]
        item_loader.add_value('property_type', response.meta.get('property_type'))
        rent_string = response.xpath('.//*[@class="header span12"]//*[@class="price-value"]/text()').extract_first()
        rent_int = response.xpath('.//*[@class="header span12"]//*[contains(@class,"rentfreqency")]/text()').extract_first()
        if 'per month' in rent_int.lower():
            item_loader.add_value('rent_string', rent_string)
        elif 'per week' in rent_int.lower():
            rent = extract_rent_currency(rent_string, OsullivanpropertySpider)*4
            item_loader.add_value('rent_string', '£'+str(rent))
        
        features = ' '.join(response.xpath('.//*[@class="features"]//li/text()').extract())

        room_count = response.xpath('.//*[@class="fa fa-bed"]/../following-sibling::p/text()').extract_first()
        if room_count:
            rooms = str(extract_number_only(room_count, thousand_separator=',', scale_separator=','))
            # https://osullivanproperty.co.uk/property/flat-to-rent-in-pembridge-villas/101709000743
            if rooms != '0' and rooms.isnumeric():
                item_loader.add_value('room_count', rooms)
            elif rooms == '0' or not rooms.isnumeric():
                if any(i in titles_join.lower() for i in studio_types) \
                        or any(i in features.lower() for i in studio_types) \
                        or any(i in description.lower() for i in studio_types):
                    item_loader.add_value('room_count', '1')
        else:
            if any(i in titles_join.lower() for i in studio_types) \
                    or any(i in features.lower() for i in studio_types) \
                    or any(i in description.lower() for i in studio_types):
                item_loader.add_value('room_count', '1')

        bathroom_count = response.xpath('.//*[@class="fa fa-bathtub bathroom"]/../following-sibling::p/text()').extract_first()
        if bathroom_count:
            bathroom = str(extract_number_only(bathroom_count, thousand_separator=',', scale_separator=','))
            if bathroom != '0' and bathroom.isnumeric():
                item_loader.add_value('bathroom_count', bathroom)

        item_loader.add_xpath('images', './/img[@class="detail-prop-gallery"]/@src')
        item_loader.add_xpath('floor_plan_images', './/img[@class="floorplan-image"]/@src')

        available_date = response.xpath('.//*[contains(text(),"Availability:")]/../text()').extract_first()
        if available_date and 'now' in remove_white_spaces(available_date).lower():
            date_time = date.today().strftime("%d/%m/%Y")
            item_loader.add_value('available_date', format_date(date_time, "%d/%m/%Y"))

        lat_lng = response.xpath('.//*[@class="mapsEmbed"]/iframe/@src').extract_first()
        if lat_lng:
            latitude = re.search(r'(?<=q=)\d+.\d+', lat_lng).group()
            longitude = re.search(r'\D\d+.\d+(?=&z)', lat_lng).group()
            item_loader.add_value('latitude', latitude)
            item_loader.add_value('longitude', longitude)

            """
            geolocator = Nominatim(user_agent=random_user_agent())
            location = geolocator.reverse(", ".join([latitude]+[longitude]))
            item_loader.add_value('address', location.address)
            if 'city' in location.raw['address']:
                item_loader.add_value('city', location.raw['address']['city'])
            elif 'town' in location.raw['address']:
                item_loader.add_value('city', location.raw['address']['town'])
            if 'postcode' in location.raw['address']:
                item_loader.add_value('zipcode', location.raw['address']['postcode'])
            """
        zipcode = response.meta["title"].split(",")[-1].strip()
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('city', "london")
        address = response.xpath('(.//span[@class="salesdisplay"])[1]/../text()').extract_first()
        if address:
            item_loader.add_value('address', ", ".join([address, zipcode]))
        # https://osullivanproperty.co.uk/property/3-bed-flat-to-rent-in-merchant-square-east-london/101709000443
        if 'lift' in features.lower():
            item_loader.add_value('elevator', True)
        # https://osullivanproperty.co.uk/property/2-bed-flat-to-rent-in-legacy-building-embassy-gardens/101709000625
        # balcony and swimming pool
        if 'balcony' in features.lower():
            item_loader.add_value('balcony', True)
        # https://osullivanproperty.co.uk/property/3-bed-flat-to-rent-in-lancelot-place-knightsbridge/101709000758
        if 'swimming pool' in features.lower():
            item_loader.add_value('swimming_pool', True)
        # https://osullivanproperty.co.uk/property/3-bed-flat-to-rent-in-merchant-square-east-london/101709000443
        # parking
        if 'parking' in features.lower():
            item_loader.add_value('parking', True)
        if 'washing machine' in features.lower():
            item_loader.add_value('washing_machine', True)
        if 'dishwasher' in features.lower():
            item_loader.add_value('dishwasher', True)
        # https://osullivanproperty.co.uk/property/3-bed-flat-to-rent-in-starboard-penthouse/101709000405
        # terrace
        if 'terrace' in features.lower():
            item_loader.add_value('terrace', True)
        # https://osullivanproperty.co.uk/property/3-bed-flat-to-rent-in-lancelot-place-knightsbridge/101709000758
        # furnished
        if "furnished" in features.lower() and "unfurnished" not in features.lower():
            item_loader.add_value('furnished', True)

        item_loader.add_value('landlord_name', "O'Sullivan Property")
        item_loader.add_value('landlord_phone', '0207 099 0800')
        item_loader.add_value('landlord_email', 'info@osullivanproperty.co.uk')

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Osullivanproperty_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
