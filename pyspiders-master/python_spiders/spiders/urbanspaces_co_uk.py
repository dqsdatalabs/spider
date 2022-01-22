# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader
from geopy.geocoders import Nominatim
from ..user_agents import random_user_agent
import js2xml
import lxml
from scrapy.selector import Selector


class UrbanspacesSpider(scrapy.Spider):
    name = "urbanspaces_co_uk"
    allowed_domains = ["urbanspaces.co.uk"]
    start_urls = [
        {'url': 'https://www.urbanspaces.co.uk/property-search-results/apartments-to-let-in-london',
         'property_type': 'Apartment'},
        {'url': 'https://www.urbanspaces.co.uk/property-search-results/houses-to-let-in-london',
         'property_type': 'House'}
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
        listings = response.xpath('//a[@class="each_result_details_link"]/@href').extract()
        for property_url in listings:
            property_url = response.urljoin(property_url)
            yield scrapy.Request(url=property_url,
                                 callback=self.get_property_details,
                                 meta={'request_url': property_url,
                                       'property_type': response.meta.get('property_type')}
                                 )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        parking_space = response.xpath("//h1/text()").get()
        if "parking" in parking_space.lower():
            return

        item_loader.add_value('property_type', response.meta.get('property_type'))
        external_id=response.meta.get('request_url').split('-')[-1]
        item_loader.add_value('external_id', external_id)
        item_loader.add_xpath('title', './/div[@id="property_title"]//text()')
        item_loader.add_xpath('description', './/div[@id="property_content"]/p/text()')

        javascript = response.xpath('.//script[contains(text(), "google.maps.LatLng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            latitude = xml_selector.xpath('.//identifier[@name="LatLng"]/../../../arguments/number/@value').extract()[0]
            longitude = xml_selector.xpath('.//identifier[@name="LatLng"]/../../../arguments/number/@value').extract()[1]
            item_loader.add_value('latitude', latitude)
            item_loader.add_value('longitude', longitude)

            """
            geolocator = Nominatim(user_agent=random_user_agent())
            coordinatesString = latitude+", "+longitude
            reverseLocation = geolocator.reverse(coordinatesString)
            item_loader.add_value('address', reverseLocation.address)
            if 'city' in reverseLocation.raw['address'].keys():
                item_loader.add_value('city', reverseLocation.raw['address']['city'])
            elif 'town' in reverseLocation.raw['address'].keys():
                item_loader.add_value('city', reverseLocation.raw['address']['town'])
            if 'postcode' in reverseLocation.raw['address'].keys():
                item_loader.add_value('zipcode', reverseLocation.raw['address']['postcode'])
            """

        # address
        javascript = response.xpath('.//script[contains(text(), "display_address")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            address = selector.xpath('.//identifier[@name="display_address"]/../..//string/text()').extract_first()
            if "," in address:
                zipcode = address.split(",")[-1].replace("Bankside","").strip()
                if not zipcode.replace(" ","").isalpha():
                    item_loader.add_value("zipcode", zipcode)
            elif ' ' in address:
                zipcode = address.split()[-1].strip()
                item_loader.add_value("zipcode", zipcode)
            item_loader.add_value('address', address)
        item_loader.add_value("city", "London")

        room_count = response.xpath('.//li[contains(text(),"Bedroom(s)")]/text()').extract_first()
        if room_count:
            room_count = room_count.split(':')[1].strip()
            item_loader.add_value('room_count', room_count)
        bathroom_count = response.xpath('.//li[contains(text(),"Bathroom(s)")]/text()').extract_first()
        if bathroom_count:
            bathroom_count = bathroom_count.split(':')[1].strip()
            item_loader.add_value('bathroom_count', bathroom_count)

        rating_value = response.xpath('.//div[@id="div_epc"]/img/@src').extract_first()
        if rating_value and len(re.findall(r"(?<=currentenergy=)\d+", rating_value)) > 0:
            rating_value = re.findall(r"(?<=currentenergy=)\d+", rating_value)[0]
            rating_value = int(rating_value)
            if rating_value >= 92 and rating_value <= 100:
                epc_class = 'A'
            elif rating_value >= 81 and rating_value <= 91:
                epc_class = 'B'
            elif rating_value >= 69 and rating_value <= 80:
                epc_class = 'C'
            elif rating_value >= 55 and rating_value <= 68:
                epc_class = 'D'
            elif rating_value >= 39 and rating_value <= 54:
                epc_class = 'E'
            elif rating_value >= 21 and rating_value <= 38:
                epc_class = 'F'
            elif rating_value >= 1 and rating_value <= 20:
                epc_class = 'G'
            item_loader.add_value('energy_label', epc_class)

        features = response.xpath('//div[@id="property_content"]//li/text()').extract()
        featuresString = " ".join(features)
        # https://www.urbanspaces.co.uk/property-details/5-Bedroom-Town-house-to-rent-on-Wallside-Barbican-EC2Y-PB8080
        if "parking" in featuresString.lower():
            item_loader.add_value('parking', True)

        # https://www.urbanspaces.co.uk/property-details/2-Bedroom-Flat-available-to-rent-on-York-Way-Kings-Cross-N1-PB9001
        if "elevator" in featuresString.lower() or 'lift' in featuresString.lower():
            item_loader.add_value('elevator', True)

        # https://www.urbanspaces.co.uk/property-details/2-Bedroom-Flat-to-rent-on-Wharf-Street-Deptford-SE8-PB8734
        if "balcony" in featuresString.lower():
            item_loader.add_value('balcony', True)

        # https://www.urbanspaces.co.uk/property-details/4-Bedroom-Mews-house-available-to-rent-on-Pepys-House-Park-Street-SE1-PB9066
        if "terrace" in featuresString.lower():
            item_loader.add_value('terrace', True)

        if "swimming pool" in featuresString.lower():
            item_loader.add_value('swimming_pool', True)

        if "washing machine" in featuresString.lower():
            item_loader.add_value('washing_machine', True)

        if "dishwasher" in featuresString.lower():
            item_loader.add_value('dishwasher', True)
        # https://www.urbanspaces.co.uk/property-details/2-Bedroom-Flat-to-rent-on-Wharf-Street-Deptford-SE8-PB8734
        if " furnished" in featuresString.lower() and "unfurnished" not in featuresString.lower():
            item_loader.add_value('furnished', True)
        # https://www.urbanspaces.co.uk/property-details/1-Bedroom-Flat-to-rent-on-King-Edwards-Road-Hackney-E9-PB9100
        elif ("unfurnished" in featuresString.lower() or "un-furnished" in featuresString.lower()) and " furnished" not in featuresString.lower():
            item_loader.add_value('furnished', False)

        rent_string = response.xpath('.//div[@class="tenancy_fees_in_title"]/text()').extract_first().split()[0]
        item_loader.add_value("rent_string", rent_string)

        item_loader.add_xpath('images', './/*[@class="thumbnail_each"]/@data-background-image')
        floor_plan_images = response.xpath('.//div[@id="property_floorplan"]/img/@src').extract()
        floor_plan_images = [response.urljoin(i) for i in floor_plan_images]
        item_loader.add_value('floor_plan_images', floor_plan_images)

        item_loader.add_value("external_link", response.meta.get('request_url'))
        item_loader.add_value('landlord_name', "Urban Spaces")
        item_loader.add_value('landlord_email', "clientservices@urbanspaces.co.uk")
        item_loader.add_value('landlord_phone', "020 7251 4000")
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.split('_')[0].capitalize(), self.country, self.locale))

        self.position += 1
        item_loader.add_value("position", self.position)
        yield item_loader.load_item()