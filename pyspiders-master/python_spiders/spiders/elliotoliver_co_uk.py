# -*- coding: utf-8 -*-
# Author: Pavit Kaur
# Team: Sabertooth

import scrapy
import re
from ..loaders import ListingLoader
from ..helper import extract_number_only, extract_rent_currency
from geopy.geocoders import Nominatim
from ..user_agents import random_user_agent


class ElliotoliverSpider(scrapy.Spider):
    name = "elliotoliver_co_uk"
    allowed_domains = ["elliotoliver.co.uk"]
    start_urls = [
        {'url': 'https://elliotoliver.co.uk/property-list-view?p_department=RL&propertyAddress=Any+Location&propertyType=1&minimumBedrooms=&minimumPrice=&minimumRentFrequency=pcm&maximumPrice=&maximumRentFrequency=pcm',
         'property_type': 'house'},
        {'url': 'https://elliotoliver.co.uk/property-list-view?p_department=RL&propertyAddress=Any+Location&propertyType=2&minimumBedrooms=&minimumPrice=&minimumRentFrequency=pcm&maximumPrice=&maximumRentFrequency=pcm',
         'property_type': 'apartment'},
        {'url': 'https://elliotoliver.co.uk/property-list-view?p_department=RL&propertyAddress=Any+Location&propertyType=3&minimumBedrooms=&minimumPrice=&minimumRentFrequency=pcm&maximumPrice=&maximumRentFrequency=pcm',
         'property_type': 'house'}
    ]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    """
    This is required to scrape complete data since this website has redirects to same page which leads to stop 
    crawler upon crawling first page of each url
    """
    custom_settings = {"DUPEFILTER_CLASS": "scrapy.dupefilters.BaseDupeFilter"}

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@class="address"]/a/@href').extract()
        for property_url in listings:
            yield scrapy.Request(url=property_url,
                                 callback=self.get_property_details,
                                 meta={'request_url': property_url,
                                       'property_type': response.meta.get('property_type')}
                                 )
        next_page_url = response.xpath('//*[@class="nextpostslink"]/@href').get()
        if next_page_url:
            yield response.follow(
                url=next_page_url,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
             )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', response.meta.get('property_type'))
        title = response.xpath('.//h1[@class="margin center"]/text()').extract_first().strip(" -")
        item_loader.add_value('title', title)

        description = response.xpath('.//h2[@class="detailsTitle"]/following-sibling::text()').extract()
        description = [text_i.strip() for text_i in description if text_i.strip() != ""][:1]
        description.extend(response.xpath('.//div[@class="content"]/p//text()').extract())
        description = " ".join(description)
        item_loader.add_value('description', description)

        address = response.xpath('.//h1[@class="margin center"]/text()').extract_first().strip(" -")
        """
        geolocator = Nominatim(user_agent=random_user_agent())
        location = geolocator.geocode(address)
        if location:
            coordinatesString = str(location.latitude)+", "+str(location.longitude)
            reverseLocation = geolocator.reverse(coordinatesString)
            if 'city' in reverseLocation.raw['address'].keys():
                item_loader.add_value('city', reverseLocation.raw['address']['city'])
            item_loader.add_value('address', location.address)
            if 'postcode' in reverseLocation.raw['address'].keys():
                item_loader.add_value('zipcode', reverseLocation.raw['address']['postcode'])
            item_loader.add_value("latitude", str(location.latitude))
            item_loader.add_value("longitude", str(location.longitude))
        else:
        """
        item_loader.add_value('address', address)
        city_zip = address.split(',')[-1]
        if any(char.isdigit() for char in city_zip):
            if len(city_zip.split(' ')) == 4:
                item_loader.add_value('zipcode', ' '.join(city_zip.split(' ')[-2:]))
                item_loader.add_value('city', (city_zip.split(' ')[1]))
            elif len(city_zip.split(' ')) == 3:
                item_loader.add_value('zipcode', city_zip)
                item_loader.add_value('zipcode', address.split(',')[-2])

        features = response.xpath('//section[@class="features"]//li/text()').extract()
        featuresString = " ".join(features)

        number_mappping = {"one": "1", "two": "2", "three": "3", "four": "4", "five": "5", "six": "6",
                           "seven": "7", "eight": "8", "nine": "9", "ten": "10"}
        room_count = featuresString.lower() + " " + description.lower()
        for key_i in number_mappping.keys():
            room_count = room_count.replace(key_i, number_mappping[key_i])
        if len(re.findall(r"[^\d]\d+[^\w]*bedroom", room_count)) > 0:
            item_loader.add_value('room_count', extract_number_only(re.findall(r"[^\d]\d+[^\w]*bedroom", room_count)[0]))
        if len(re.findall(r"[^\d]\d+[^\w]*bathroom", room_count)) > 0:
            item_loader.add_value('bathroom_count', extract_number_only(re.findall(r"[^\d]\d+[^\w]*bathroom", room_count)[0]))

        # example:https://elliotoliver.co.uk/property/strickland-road-cheltenham-gl52-6rr-3
        if "parking" in featuresString.lower():
            item_loader.add_value('parking', True)

        # https://elliotoliver.co.uk/property/albion-street-cheltenham-gl52-2lp
        if "elevator" in featuresString.lower() or 'lift' in featuresString.lower():
            item_loader.add_value('elevator', True)

        # https://elliotoliver.co.uk/property/st-michaels-close-cheltenham-gl53-9dw
        if "balcony" in featuresString.lower():
            item_loader.add_value('balcony', True)

        # example:https://elliotoliver.co.uk/property/monson-avenue-cheltenham-gl50-4eh-2
        if "terrace" in featuresString.lower():
            item_loader.add_value('terrace', True)

        if "swimming pool" in featuresString.lower():
            item_loader.add_value('swimming_pool', True)

        # https://elliotoliver.co.uk/property/albion-street-cheltenham-gl52-2lp
        if "washing machine" in featuresString.lower():
            item_loader.add_value('washing_machine', True)

        if "dishwasher" in featuresString.lower():
            item_loader.add_value('dishwasher', True)

        # example:https://elliotoliver.co.uk/property/strickland-road-cheltenham-gl52-6rr-3
        if " furnished" in featuresString.lower():
            item_loader.add_value('furnished', True)
        # https://elliotoliver.co.uk/property/long-mynd-avenue-cheltenham-gl51-3qn
        elif "unfurnished" in featuresString.lower():
            item_loader.add_value('furnished', False)

        item_loader.add_xpath("rent_string", './/div[@id="titleMargin"]//span/text()')
        item_loader.add_xpath('images', './/li[contains(@id,"image-")]//img/@src')
        item_loader.add_xpath('floor_plan_images', './/a[contains(text(),"Floorplans")]/@href')
        item_loader.add_value("external_link", response.meta.get('request_url'))
        item_loader.add_value('landlord_name', "Elliot Oliver Estate Agents")
        item_loader.add_value('landlord_email', "info@elliotoliver.co.uk")
        item_loader.add_value('landlord_phone', "01242 321091")

        self.position += 1
        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", "Elliotoliver_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()

