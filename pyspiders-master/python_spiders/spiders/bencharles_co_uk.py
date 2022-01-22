# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date, remove_white_spaces, remove_unicode_char
import re
import lxml 
import js2xml
from scrapy import Selector
import dateparser


class Bencharles(scrapy.Spider):
    name = "bencharles_co_uk"
    allowed_domains = ["bencharles.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = ["https://www.bencharles.co.uk/search/?view=list&department=residential-lettings"]
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url})

    def parse(self, response, **kwargs):
        listings = response.xpath('//div[@class="bceathumbinner"]/../@href').extract()
        for url in listings:
            yield scrapy.Request(
                url=url,
                callback=self.get_property_details,
                meta={'request_url': url})
        
        if len(listings) == 10:
            next_page_url = response.xpath('//a[@class="next page-numbers"]/@href').extract_first()
            if next_page_url:
                yield scrapy.Request(
                        url=next_page_url,
                        callback=self.parse,
                        meta={'request_url': next_page_url})

    def get_property_details(self, response):
        
        address = response.xpath('//p[@class="property_title entry-title"]//text()').extract_first()
        features = " ".join(response.xpath('//div[@class="features"]//li/text()').extract())
        description = " ".join(response.xpath('//p[@class="room"]//text()').extract())
        external_id = response.xpath('//li[@class="ref"]/text()').extract_first()
        if external_id:
            external_id = external_id.split(':')[1].strip()
        
        item_loader = ListingLoader(response=response)

        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', external_id)
        item_loader.add_xpath('title', '//p[@class="property_title entry-title"]/../h1/text()')
        item_loader.add_value('address', address)
        if address:
            city = address.split(',')[-2].strip()
            zipcode = address.split(',')[-1].strip()
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
        rent = extract_number_only(response.xpath('//div[@class="offersinregion"]/p/text()').extract_first(),thousand_separator=',',scale_separator='.')
        if rent:
            rent_pw = response.xpath('//div[@class="offersinregion"]/p/text()').extract_first()
            if 'pw' in rent_pw:
                rent = str(int(rent) * 4)
        
        item_loader.add_value('rent_string', '£'+rent)

        item_loader.add_xpath('images', '//a[@class="propertyhive-main-image"]/@href')
        item_loader.add_xpath('floor_plan_images', '//div[@id="floorplan"]//img/@src')
        item_loader.add_value('description', description)

        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex"]
        house_types = ['chalet', 'bungalow', 'maison', 'house', 'home', 'villa']
        studio_types = ["studio"]
        property_type_text = response.xpath('//li[@class="type"]/text()').extract_first()
        property_type = None
        if property_type_text:
            property_type_text = re.sub(r"type[^\w]*", "", property_type_text.lower())
            if any([i in property_type_text for i in apartment_types]):
                property_type = "apartment"
                if "studio" in description:
                    property_type = "studio_apartment"
            elif any([i in property_type_text for i in house_types]):
                property_type = "house"
            elif any([i in property_type_text for i in studio_types]):
                property_type = "studio"
        elif 'house' in description:
            property_type = "house"
        item_loader.add_value('property_type', property_type)
        
        room_count = response.xpath('//li[@class="bedrooms"]/text()').extract_first()
        if room_count:
            item_loader.add_value('room_count', extract_number_only(room_count))

        bathroom_count = response.xpath('//li[@class="bathrooms"]/text()').extract_first()
        if bathroom_count:
            item_loader.add_value('bathroom_count', extract_number_only(bathroom_count))

        available_date=response.xpath("substring-after(//ul/li[@class='available']/text(),': ')").get()
        if available_date:
            date2 =  available_date.strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        javascript = response.xpath('.//script[contains(text(),"initMap")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            item_loader.add_value('latitude', xml_selector.xpath('.//property[@name="lat"]/number/@value').extract_first())
            item_loader.add_value('longitude', xml_selector.xpath('.//property[@name="lng"]/number/@value').extract_first())
        
        # https://www.bencharles.co.uk/property/dur200415_l/
        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)
        # https://www.bencharles.co.uk/property/dur200388_l/
        if "balcony" in features.lower():
            item_loader.add_value('balcony', True)
        if "washing machine" in features.lower():
            item_loader.add_value('washing_machine', True)
        if "dishwasher" in features.lower():
            item_loader.add_value('dishwasher', True)
        # https://www.bencharles.co.uk/property/dur200415_l/
        if "parking" in features.lower():
            item_loader.add_value('parking', True)
        else:
            parking = "".join(response.xpath("//table//tr//td/b[contains(.,'Parking') or contains(.,'Garage')]/text()").extract())
            if parking:
                item_loader.add_value('parking', True)
        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)

        if "furnished" in features.lower():
            if "unfurnished" in features.lower():
                item_loader.add_value('furnished', False)
            else:
                item_loader.add_value('furnished', True)

        # https://www.bencharles.co.uk/property/dur120409_l/
        if "furnished" in description.lower():
            if "unfurnished" in description.lower():
                item_loader.add_value('furnished', False)
            else:
                item_loader.add_value('furnished', True)

        item_loader.add_value('landlord_phone', '0191 383 0011')
        item_loader.add_value('landlord_name', 'Bencharles')
        item_loader.add_value('landlord_email', 'durham@bencharles.co.uk')

        if not item_loader.get_collected_values("furnished"):
            furnished = response.xpath("//li[@class='furnished']/text()").get()
            if furnished and "unfurnished" in furnished.split(":")[-1].lower(): item_loader.add_value('furnished', False)
            elif furnished and "furnished" in furnished.split(":")[-1].lower(): item_loader.add_value('furnished', True)

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Bencharles_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
