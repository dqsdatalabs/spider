# -*- coding: utf-8 -*-
# Author: Praveen Chaudhary
# Team: Sabertooth

import lxml
import scrapy
import js2xml
import re

from scrapy import Selector

from ..loaders import ListingLoader
from ..helper import  format_date, convert_string_to_numeric
from geopy.geocoders import Nominatim
from ..user_agents import random_user_agent


class DreamviewestatesSpider(scrapy.Spider):
    name = 'dreamviewestates_co_uk'
    allowed_domains = ['dreamviewestates.co.uk']
    start_urls = ['https://dreamviewestates.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    external_source = "Dreamviewestates_PySpider_united_kingdom_en"

    def start_requests(self):
        start_urls = ["https://dreamviewestates.co.uk/index.php?option=com_propertylab&view=propertylab&layout=propertysearch&type=tolet&map=0&showview=grid&Itemid=166"]
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@class="row1"]/a/@href').extract()
        for property_item in listings:
            yield scrapy.Request(
                url=f"https://dreamviewestates.co.uk{property_item}",
                callback=self.get_property_details,
                meta={'request_url': f"https://dreamviewestates.co.uk{property_item}"}
            )
        next_page_url = response.xpath('.//a[contains(text()," > ")]/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(
                url=response.urljoin(f"https://dreamviewestates.co.uk{next_page_url}"),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    def get_property_details(self, response):
        description = ''.join(response.xpath('.//div[contains(@class,"para-div")]/p/text()').extract())
        tel = response.xpath('.//a[contains(@href,"tel")]/text()').extract_first().split(":  ")[-1]
        rent = int(convert_string_to_numeric(response.xpath('.//span[@class="price_text"]/text()').extract_first(), DreamviewestatesSpider))*4
        
        address = response.xpath('.//h3[@class="addresstitle"]/text()').extract_first()

        item_loader = ListingLoader(response=response)
        property_type = None
        if "studio" in description.lower():
            property_type = "studio"
        elif "flat" in description.lower():
            property_type = "apartment"
        elif "house" in description.lower():
            property_type = "house"
        # set because rest are rooms only
        else:
            property_type = "house"

        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', re.findall(r"(?<=&id=)\d+", response.meta.get('request_url'))[0])
        item_loader.add_value('title', property_type+' at '+address)
        item_loader.add_value('address', address)
        item_loader.add_value('rent_string', f"£{rent}")
        item_loader.add_value('description', description)

        item_loader.add_xpath('images', './/a[contains(@class,"rsImg")]/@href')
        item_loader.add_value('landlord_name', 'Dreamview Estates')
        item_loader.add_value('landlord_email', 'mail@dreamviewestates.co.uk')
        item_loader.add_value('landlord_phone', tel)

        square_meters = response.xpath('.//li[contains(text(),"SQ") or contains(text(),"sq")]/text()').extract_first()
        if square_meters:
            # if sq is given in both sq feet and meters
            if "/" in square_meters:
                sq = convert_string_to_numeric(square_meters.split("/")[-1], DreamviewestatesSpider)
            # if sq is given in sq feet only
            else:
                sq = int(convert_string_to_numeric(square_meters, DreamviewestatesSpider)*0.092903)
            item_loader.add_value('square_meters', sq)
        # if room and bath:
        #     room_bath_element = response.xpath('.//i[contains(@class,"bed")]/parent::span/text()').extract()
        #     item_loader.add_xpath('bathroom_count',extract_number_only(room_bath_element[1]))
        #     item_loader.add_xpath('room_count',extract_number_only(room_bath_element[0]))
        # if room:
        #     room_bath_element = ''.join(response.xpath('.//i[contains(@class,"bed")]/parent::span/text()').extract())
        #     item_loader.add_xpath('room_count',extract_number_only(room_bath_element))
        # if bath:
        #     room_bath_element = ''.join(response.xpath('.//i[contains(@class,"bath")]/parent::span/text()').extract())
        #     item_loader.add_xpath('bathroom_count', extract_number_only(room_bath_element))
        room_bath= "".join(response.xpath('.//i[contains(@class,"bath")]/preceding-sibling::text()[1]').extract())
        if room_bath:
            item_loader.add_xpath('bathroom_count', room_bath.replace("|","").strip())

        room_count = response.xpath('.//i[contains(@class,"bed")]/preceding-sibling::text()').extract_first()
        if '0' in room_count:
            item_loader.add_value('room_count', '1')
        else:
            item_loader.add_value('room_count', room_count)

        javascript = response.xpath('.//script[contains(text(),"LatLng")]/text()').extract_first()
        location = None
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            latitude = xml_selector.xpath('.//identifier[@name="LatLng"]/../../../arguments/number/@value').extract()[0]
            longitude = xml_selector.xpath('.//identifier[@name="LatLng"]/../../../arguments/number/@value').extract()[1]
            item_loader.add_value('latitude', latitude)
            item_loader.add_value('longitude', longitude)
            """
            geolocator = Nominatim(user_agent=random_user_agent())
            location = geolocator.reverse(f"{latitude}, {longitude}")
            if location and 'address' in location.raw:
                if 'postcode' in location.raw['address']:
                    item_loader.add_value('zipcode', location.raw['address']['postcode'])
                if 'city' in location.raw['address']:
                    item_loader.add_value('city', location.raw['address']['city'])
            """
        if not location:
            city_zip = item_loader.get_output_value('address').split(',')[-1].split(" ")
            zipcode = [text_i for text_i in city_zip if len(re.findall(r"\d+", text_i)) > 0]
            if len(zipcode) > 0:
                item_loader.add_value('zipcode', " ".join(zipcode))
            city = [text_i for text_i in city_zip if len(re.findall(r"\d+", text_i)) == 0]
            if len(city) > 0:
                item_loader.add_value('city', " ".join(city))

        # ex https://dreamviewestates.co.uk/index.php?option=com_propertylab&task=showproperty&id=286&perpage=5&start=0&Itemid=166
        elevator = response.xpath('.//li[contains(text(),"LIFT")]').extract_first()
        if elevator:
            item_loader.add_value('elevator', True)

        # ex https://dreamviewestates.co.uk/index.php?option=com_propertylab&task=showproperty&id=401&perpage=5&start=0&Itemid=166 present as key point in li tag
        parking = response.xpath('.//li[contains(text(),"PARKING")]').extract_first()
        if parking:
            item_loader.add_value('parking', True)

        # https://dreamviewestates.co.uk/index.php?option=com_propertylab&task=showproperty&id=361&perpage=5&start=0&Itemid=166
        furnished = response.xpath('.//li[contains(text(), "FURNISHED")]').extract_first()
        if furnished:
            item_loader.add_value('furnished', True)

        # https://dreamviewestates.co.uk/index.php?option=com_propertylab&task=showproperty&id=401&perpage=5&start=0&Itemid=166
        available_date = response.xpath('.//li[contains(text(),"AVAILABLE")]').extract_first()
        if available_date:
            date = re.findall(r'\d{2}[\/\-]\d{2}[\/\-]\d{2}',available_date)
            if date:
                item_loader.add_value('available_date', format_date(date[0], "%d/%m/%y"))

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", self.external_source)
        yield item_loader.load_item()
