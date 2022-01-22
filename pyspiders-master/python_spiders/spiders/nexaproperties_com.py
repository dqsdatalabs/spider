# -*- coding: utf-8 -*-
# Author: Pavit Kaur
# Team: Sabertooth

import scrapy

from ..loaders import ListingLoader
from geopy.geocoders import Nominatim
from ..user_agents import random_user_agent


class NexapropertiesSpider(scrapy.Spider):
    name = "nexaproperties_com"
    allowed_domains = ["nexaproperties.com"]
    start_urls = ['https://www.nexaproperties.com/properties/properties-grid-view?min-price=&max-price=&type=any&location=any&status=for-rent&bedrooms=any&bathrooms=any']

    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse)

    def parse(self, response, **kwargs):
        listings = response.xpath('.//a[contains(@href,"/property/")]/@href').extract()
        listings = set(listings)
        for property_url in listings:
            property_url = response.urljoin(property_url)
            yield scrapy.Request(url=property_url,
                                 callback=self.get_property_details,
                                 meta={'request_url': property_url}
                                 )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        property_type_map = {"flats / apartments": "apartments"}
        property_type = response.xpath('.//span[@class="property-type"]/text()').extract_first()
        if property_type:
            property_type = property_type.lower()
            for key_i in property_type_map:
                property_type = property_type.replace(key_i, property_type_map[key_i])
            item_loader.add_value('property_type', property_type)

        external_id = response.xpath('.//div[@class="property-id"]/b/text()').extract_first()
        item_loader.add_value('external_id', external_id)

        item_loader.add_xpath('title', './/h2[@class="casaroyal-property-title"]/text()')
        item_loader.add_xpath('description', './/div[@class="property-address-content"]//text()')

        city = response.xpath('.//strong[contains(text(),"City")]/following-sibling::text()').extract_first().strip()
        item_loader.add_value('city', city)

        zipcode = response.xpath('.//strong[contains(text(),"Post code")]/following-sibling::text()').extract_first().strip()
        item_loader.add_value('zipcode', zipcode)

        addressTitle = response.xpath('.//h2[@class="casaroyal-property-title"]/text()').extract_first()
        addressTitle = ", ".join(addressTitle.split(",")[1:])
        address = addressTitle.strip()+", "+city+", "+zipcode
        item_loader.add_value('address', address)

        item_loader.add_xpath('latitude', '//div[contains(@class,"property-item-data clearfix")]/@data-lat')
        item_loader.add_xpath('longitude', '//div[contains(@class,"property-item-data clearfix")]/@data-long')

        """
        geolocator = Nominatim(user_agent=random_user_agent())
        location = geolocator.geocode(address)
        if location:
            item_loader.add_value('address', location.address)
            if not item_loader.get_output_value('latitude'):
                item_loader.add_value('latitude', str(location.latitude))
            if not item_loader.get_output_value('longitude'):
                item_loader.add_value('longitude', str(location.longitude))
        else:
            item_loader.add_value('address', address)
        """

        item_loader.add_xpath('room_count', './/span[contains(text(),"Bed")]/preceding-sibling::span/text()')
        item_loader.add_xpath('bathroom_count', './/span[contains(text(),"Bath")]/preceding-sibling::span/text()')

        features = response.xpath('//div[@class="features"]//li/text()').extract()
        if features:
            featuresString = " ".join(features)
            # https://www.nexaproperties.com/property/1-bedrooms-overs-60s-retirement-apartment-at-waterlooville-3
            if "parking" in featuresString.lower():
                item_loader.add_value('parking', True)
            # https://www.nexaproperties.com/property/1-bedrooms-upper-floor-flat-flat-at-portsmouth
            if "elevator" in featuresString.lower() or 'lift' in featuresString.lower():
                item_loader.add_value('elevator', True)

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

            # https://www.nexaproperties.com/property/1-bedrooms-en-suite-bedroom-in-shared-accommodation-at-portsmouth
            if " furnished" in featuresString.lower():
                item_loader.add_value('furnished', True)
            # https://www.nexaproperties.com/property/2-bedrooms-mid-terraced-house-at-portsmouth
            elif "unfurnished" in featuresString.lower() or "un-furnished" in featuresString.lower():
                item_loader.add_value('furnished', False)

            # https://www.nexaproperties.com/property/2-bedrooms-mid-terraced-house-at-portsmouth
            if "pets considered" in featuresString.lower():
                item_loader.add_value('pets_allowed', True)

        rent_currency = response.xpath('.//span[@class="property-price-holder"]//text()').extract_first()
        rent_value = response.xpath('.//span[@class="property-price-number"]//text()').extract_first()
        item_loader.add_value("rent_string", rent_currency + rent_value)

        item_loader.add_xpath('images', './/div[@class="carousel-slider"]//img/@src')

        floor_plan_images = response.xpath('.//ul[@class="attachments-list clearfix"]//a/@href').extract()
        floor_plan_images = [i for i in floor_plan_images if "FLP" in i]
        item_loader.add_value('floor_plan_images', floor_plan_images)

        item_loader.add_value("external_link", response.meta.get('request_url'))
        item_loader.add_xpath('landlord_name', './/div[@id="print_page_content"]//div[@class="property-agent-details"]/h3/text()')
        item_loader.add_xpath('landlord_email', './/div[@id="print_page_content"]//span[contains(text(),"Email")]/following-sibling::text()')
        item_loader.add_xpath('landlord_phone', './/div[@id="print_page_content"]//li[@class="mobile"]/text()')

        # https://www.nexaproperties.com/property/1-bedrooms-upper-floor-flat-flat-at-portsmouth
        if not item_loader.get_output_value('landlord_name') \
            and not item_loader.get_output_value('landlord_email') \
            and not item_loader.get_output_value('landlord_phone'):
            item_loader.add_value('landlord_name', "NEXA Properties")
            item_loader.add_value('landlord_email', "hello@nexaproperties.com")
            item_loader.add_value('landlord_phone', "+44 (0)2392 295046")

        self.position += 1
        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", "Nexaproperties_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
