# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import js2xml
import re
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date, remove_white_spaces
from geopy.geocoders import Nominatim
from ..user_agents import random_user_agent
import lxml.etree
from parsel import Selector
import scrapy, copy, urllib
from word2number import w2n

class RedacstrattonsSpider(scrapy.Spider):
    name = 'redacstrattons_com'
    allowed_domains = ['www.redacstrattons.com']
    start_urls = ['https://www.redacstrattons.com/']
    execution_type = 'testing'
    country = 'united_kingdom'
    thousand_separator = ','
    scale_separator = '.'
    locale = 'en'
    api_url = 'https://www.redacstrattons.com/en/lettings/to_rent/property_search_results.aspx'
    params = {'p': 1}
    position = 0

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.redacstrattons.com/en/lettings/to_rent/property_search_results.aspx?p=1&hf=H',
                'property_type': 'house'},
            {'url': 'https://www.redacstrattons.com/en/lettings/to_rent/property_search_results.aspx?p=1&hf=f',
             'property_type': 'apartment'}
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'request_url': url.get('url'),
                                       'params': self.params,
                                       'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        for property_url in response.xpath('.//*[@class="b_view"]//a/@href').extract():
            yield scrapy.Request(
                url=response.urljoin(property_url),
                callback=self.get_property_details,
                meta={'request_url': response.urljoin(property_url),
                      'property_type': response.meta.get('property_type')}
            )

        next_page_url = response.xpath('.//div[@class="pagination_pages"]/a[contains(text(),"Next")]/@href').extract_first()
        if next_page_url:
            next_page_url = response.urljoin(next_page_url)
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'property_type': response.meta.get('property_type')}
            )
            
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        external_link = response.meta.get('request_url')
        item_loader.add_value('external_link', external_link)
        external_id = external_link.split('=')[1]
        item_loader.add_value('external_id', external_id)

        rental_str = response.xpath(".//*[@class='rental_price']/text()").extract_first()
        if rental_str:
            rent_string = rental_str.split("(")[0]
            item_loader.add_value('rent_string', rent_string)
        property_type = response.meta.get('property_type')
        titles = response.xpath('.//h1//text()').extract()
        title = ' '.join(titles)
        if title:
            item_loader.add_value('title', title)
            if "studio" in title.lower():
                property_type = "studio"
        item_loader.add_value('property_type', property_type)

        item_loader.add_xpath('images', './/*[@id="galleria"]//a/@href')
        landlord_phone = response.xpath("//div[@class='property_detail_header_right']/p/text()").extract()
        item_loader.add_value('landlord_phone', landlord_phone[0].replace('Call:', ''))
        item_loader.add_value('landlord_name', "Redac Strattons")
        item_loader.add_xpath('description', './/h3[contains(text(),"Description")]/following-sibling::p/text()')
        item_loader.add_xpath('bathroom_count', './/dt[contains(text(),"Bathroom")]/following-sibling::dd[1]/text()[.!="0"]')
        item_loader.add_xpath('square_meters', './/span[contains(text(),"Area")]/following-sibling::span/text()')

        # this has been done because sqft is also present in the same field
        square_meters = response.xpath('.//*[contains(text(),"m²")]/text()').extract_first()
        if square_meters:
            square = re.search(r'\d+(?=m²)', square_meters)
            if square:
                item_loader.add_value('square_meters', square.group())


        room_count = response.xpath('.//dt[contains(text(),"Bedroom")]/following-sibling::dd[1]/text()').extract_first()
        if room_count:
            room_num = room_count.split()[0].strip()
            if room_num.isalpha():
                room = w2n.word_to_num(room_num.strip().split(" ")[0])
                item_loader.add_value('room_count', str(room))
            else:
                item_loader.add_value('room_count', room_num)
        elif property_type == "studio":
            item_loader.add_value('room_count', "1")
        # elif 'bedroom' in title:
        #     room_num = re.search(r'\w+\s(?=bedroom)',title).group().strip()
        #     if room_num.isalpha():
        #         # room = w2n.word_to_num(room_num)
        #         item_loader.add_value('room_count',str(room_num))
        #     else:
        #         item_loader.add_value('room_count',room_num)
        
        furnished = response.xpath('.//dt[contains(text(),"Furnished")]/following-sibling::dd/text()').extract_first()
        if furnished and furnished == 'Furnished':
            item_loader.add_value('furnished', True)
        elif furnished and furnished == 'Unfurnished':
            item_loader.add_value('furnished', False)
        
        parking = response.xpath('.//dt[contains(text(),"Parking")]/following-sibling::dd/text()').extract_first()
        if parking and 'no' not in parking.lower():
            item_loader.add_value('parking', True)
        elif parking and 'no' in parking.lower():
            item_loader.add_value('parking', False)
            
        washing_machine = response.xpath(".//li[contains(text(),'Washing Machine')]/text()").extract_first()
        if washing_machine and 'no' in washing_machine.lower():
            item_loader.add_value('washing_machine', False)
        elif washing_machine:
            item_loader.add_value('washing_machine', True)
        
        dishwasher = response.xpath(".//li[contains(text(),'Dishwasher')]/text()").extract_first()
        if dishwasher and 'no' in dishwasher.lower():
            item_loader.add_value('dishwasher', False)
        elif dishwasher:
            item_loader.add_value('dishwasher', True)
        
        pets_allowed = response.xpath(".//li[contains(text(),'Pets allowed')]/text()").extract_first()
        if pets_allowed and 'no' in pets_allowed.lower():
            item_loader.add_value('pets_allowed', False)
        elif pets_allowed:
            item_loader.add_value('pets_allowed', True)
        
        terrace = response.xpath(".//li[contains(text(),'Terrace')]/text()").extract_first()
        if terrace and 'no' in terrace.lower():
            item_loader.add_value('terrace', False)
        elif terrace:
            item_loader.add_value('terrace', True)
        
        balcony = response.xpath(".//li[contains(text(),'Balcony')]/text()").extract_first()
        if balcony and 'no' in balcony.lower():
            item_loader.add_value('balcony', False)
        elif balcony:
            item_loader.add_value('balcony', True)
        
        elevator = response.xpath('.//li[contains(text(),"Lift")]/text()').extract_first()
        if elevator and 'no' in elevator.lower():
            item_loader.add_value('elevator', False)
        elif elevator:
            item_loader.add_value('elevator', True)

        floor = response.xpath('.//dt[contains(text(),"Type")]/following-sibling::dd/text()').extract_first()
        if floor:
            floor_num = re.search(r'\d+.*\s(?=floor)', floor)
            if floor_num:
                floor_num = floor_num.group().strip()
                item_loader.add_value('floor', extract_number_only(floor_num))

        javascript = response.xpath('.//script[contains(text(),"lat")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            latitude = selector.xpath('.//property [@name="lat"]//text()').extract()
            longitude = selector.xpath('.//property [@name="lng"]//text()').extract()
            if latitude and longitude:
                item_loader.add_value('latitude', latitude[0])
                item_loader.add_value('longitude', longitude[0])

        address = response.xpath('.//*[@class="rental_price"]/following-sibling::p/text()').extract_first()
        if address:
            item_loader.add_value('address', address)
            zipcode = address.split(' ')[-1]
            if address.split(' ')[-2].isalpha():
                item_loader.add_value('city', address.split(' ')[-2])
                item_loader.add_value('zipcode',zipcode)
            else:
                item_loader.add_value('city', address.split(' ')[-3])
                item_loader.add_value('zipcode',address.split(' ')[-1] + " "+zipcode)
        elif item_loader.get_output_value('latitude') and item_loader.get_output_value('longitude'):
            geolocator = Nominatim(user_agent=random_user_agent())
            coordinates = ', '.join(item_loader.get_output_value('latitude')+item_loader.get_output_value('longitude'))
            location = geolocator.reverse(coordinates)
            if location:
                item_loader.add_value('address', location.address)
                if "postcode" in location.raw["address"]:
                    item_loader.add_value('zipcode', location.raw["address"]["postcode"])
                if "city" in location.raw["address"]:
                    item_loader.add_value('city', location.raw["address"]["city"])
            
        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Redacstrattons_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
