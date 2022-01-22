# -*- coding: utf-8 -*-
# Author: Pankaj Kalania
# Team: Sabertooth

import scrapy, copy, urllib
from ..loaders import ListingLoader
from ..helper import extract_number_only, extract_rent_currency, format_date
from geopy.geocoders import Nominatim
import re
import js2xml
import lxml.etree
import math
from parsel import Selector
from ..user_agents import random_user_agent
from datetime import date, datetime


class ReliablehomesSpider(scrapy.Spider):
    name = 'reliablehomes_co_uk'
    allowed_domains = ['reliablehomes.co.uk']
    start_urls = ['http://www.reliablehomes.co.uk']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    position = 0
    thousand_separator = ','
    scale_separator = '.'
        
    def start_requests(self):
        start_urls = ['https://reliablehomes.co.uk/property-search/page/1/?department=residential-lettings']
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url})
            
    def parse(self, response, **kwargs):
        for property_url in response.xpath('.//*[@class="thumbnail"]//a/@href').extract():
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'request_url': property_url})
        
        if len(response.xpath('.//*[@class="thumbnail"]//a')) > 0:
            current_page = re.findall(r"(?<=page/)\d+", response.meta["request_url"])[0]
            next_page_url = re.sub(r"(?<=page/)\d+", str(int(current_page) + 1), response.meta["request_url"])
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse,
                meta={'request_url': next_page_url}
            )
            
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('request_url'))
        external_id = response.xpath('.//*[contains(text(),"Ref:")]/text()').extract_first()
        item_loader.add_value('external_id', extract_number_only(external_id))
        item_loader.add_xpath('title', './/*[@class="meta-title nomobilephone"]/text()')
        item_loader.add_xpath('description', './/h2[contains(text(),"Property Details")]/following-sibling::text()')
        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex"]
        house_types = ['chalet', 'bungalow', 'maison', 'house', 'home', 'villa']
        studio_types = ["studio"]
        property_type = response.xpath('.//*[contains(text(),"Type:")]/text()').extract_first()
        if property_type:
            prop = property_type.split(':')[1].strip()
            if prop.lower() in apartment_types + house_types + studio_types:
                if prop.lower() in apartment_types:
                    item_loader.add_value('property_type', "apartment")
                elif prop.lower() in house_types:
                    item_loader.add_value('property_type', "house")
                elif prop.lower() in studio_types:
                    item_loader.add_value('property_type', 'studio')
                
                room_count = response.xpath('.//*[contains(text(),"Bedrooms:")]/text()').extract_first()
                if (room_count is None or room_count == '0') and prop.lower() == "studio":
                    item_loader.add_value('room_count', '1')
                elif room_count:
                    item_loader.add_value('room_count', extract_number_only(room_count))
                
                bathroom_count = response.xpath('.//*[contains(text(),"Bathrooms:")]/text()').extract_first()
                if bathroom_count:
                    item_loader.add_value('bathroom_count', extract_number_only(bathroom_count))

                item_loader.add_xpath('images', './/*[contains(text(),"Gallery")]/following-sibling::span//img/@src')
                item_loader.add_xpath('floor_plan_images', './/*[contains(text(),"Floorplan")]/following-sibling::img/@src')

                rent_string = response.xpath('.//div[@class="images"]//div[contains(@class, "meta-price")]/text()').extract_first()
                if rent_string:
                    if "pw" in rent_string:
                        item_loader.add_value('rent_string', "£ " + str(extract_rent_currency(rent_string, ReliablehomesSpider)[0]*4))
                    else:
                        item_loader.add_value('rent_string', rent_string)

                square_meters = response.xpath('.//li[contains(text(), "sq ft")]/text()').extract_first()
                if square_meters:
                    item_loader.add_value('square_meters', str(math.ceil(float(extract_number_only(square_meters)) * 0.092903)))

                details = response.xpath('.//*[@class="features features-box"]//li/text()').extract()
                # furnished
                # https://reliablehomes.co.uk/property/leon-house-191-green-lanes-n13-no-administration-fees-to-tenants/
                if any('unfurnished' in i.lower() for i in details):
                    item_loader.add_value('furnished', False)
                elif any('furnished' in i.lower() for i in details):
                    item_loader.add_value('furnished', True)

                # https://reliablehomes.co.uk/property/36353/
                # terrace and parking
                if any('terrace' in i.lower() for i in details):
                    item_loader.add_value('terrace', True)
                    
                if any('parking' in i.lower() for i in details):
                    item_loader.add_value('parking', True)
                    
                if any('Available Now' in i for i in details):
                    available_date = date.today().strftime("%d/%m/%Y")
                    item_loader.add_value('available_date', format_date(available_date, "%d/%m/%Y"))
                elif any('Available from' in i for i in details):
                    for i in details:
                        date_avail = re.search(r'(?<=Available from).*', i)
                        if date_avail:
                            dates = date_avail.group().strip()
                            dates_upd = re.sub('(\d+)(st|nd|rd|th)', '\g<1>', dates)
                            # https://reliablehomes.co.uk/property/york-way-london-n7/
                            # available_date
                            r = re.compile('\d{1,2} \w+ \d{4}')
                            if r.match(dates_upd):
                                d = datetime.strptime(dates_upd, '%d %B %Y').strftime('%d/%m/%Y')
                                item_loader.add_value('available_date', format_date(d, '%d/%m/%Y'))

                item_loader.add_value('landlord_phone', '020 3695 3065')
                item_loader.add_value('landlord_email', 'office@reliablehomes.co.uk')
                
                javascript = response.xpath('.//script[contains(text(), "myLatlng")]/text()').extract_first()
                if javascript:
                    xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
                    selector = Selector(text=xml)
                    lat_lng = selector.xpath('.//var[@name="myLatlng"][1]//arguments/number/@value').extract()
                    if len(lat_lng) == 2:
                        item_loader.add_value('latitude', lat_lng[0])
                        item_loader.add_value('longitude', lat_lng[1])
                        geolocator = Nominatim(user_agent=random_user_agent())
                        location = geolocator.reverse(', '.join(lat_lng))
                        if location:
                            item_loader.add_value('address', location.address)
                            if "postcode" in location.raw["address"]:
                                item_loader.add_value('zipcode', location.raw["address"]["postcode"])
                            if "city" in location.raw["address"]:
                                item_loader.add_value('city', location.raw["address"]["city"])
                
                self.position += 1
                item_loader.add_value('position', self.position)
                item_loader.add_value("external_source", "Reliablehomes_PySpider_{}_{}".format(self.country, self.locale))
                yield item_loader.load_item()
