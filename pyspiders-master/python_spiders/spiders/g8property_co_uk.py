# -*- coding: utf-8 -*-
# Author: Pavit Kaur
# Team: Sabertooth

import scrapy
import re
import js2xml
import lxml.etree
from parsel import Selector
from ..loaders import ListingLoader
from ..helper import format_date


class G8propertyCoUkSpider(scrapy.Spider):
    name = "g8property_co_uk"
    allowed_domains = ["g8property.co.uk"]
    start_urls = (
        'https://www.g8property.co.uk/notices?c=48&p=1&view_type=&min_price=&max_price=5000&&filter_attribute%5Bcategorical%5D%5B1%5D=&filter_attribute%5Bnumeric%5D%5B2%5D%5Bmin%5D=&filter_attribute%5Bnumeric%5D%5B2%5D%5Bmax%5D=&filter_attribute%5Bnumeric%5D%5B3%5D%5Bmin%5D=&filter_attribute%5Bnumeric%5D%5B3%5D%5Bmax%5D=',
        'https://www.g8property.co.uk/notices?c=44&p=1&view_type=&min_price=&max_price=5000&loc=&filter_attribute[numeric][2][min]=&filter_attribute[numeric][2][max]=&min_price=&max_price=5000&price_sort=&available_date=&filter_attribute[categorical][1]=&filter_attribute[categorical][30]=&filter_attribute[categorical][31]='
    )
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
                                 meta={'request_url': property_url})

        total_page = response.xpath('//input[@id="pagination_number"]/@total-page').extract_first()
        p = response.xpath('.//input[@id="pagination_number"]/@value').extract_first()
        if p and int(p) < int(total_page):
            current_string = f"p={p}"
            p = str(int(p)+1)
            next_page_url = response.url.replace(current_string, f"p={p}")
            yield scrapy.Request(url=next_page_url,
                                 callback=self.parse)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.xpath('.//span[text()="Property Type"]/following-sibling::text()').extract_first()
        if property_type:
            if "room" in property_type.lower():
                item_loader.add_value('property_type', "room")
            elif "studio" in property_type.lower():
                item_loader.add_value('property_type', "studio")
            elif "house" in property_type.lower():
                item_loader.add_value('property_type', "house")
            elif "apartment" in property_type.lower():
                item_loader.add_value('property_type', "apartment")

        external_id = response.xpath('//div[@class="property-id"]/b/text()').extract_first()
        item_loader.add_xpath('external_id', './/span[text()="Reference number"]/following-sibling::text()')

        item_loader.add_xpath('title', './/title/text()')
        description = response.xpath('.//div[@id="full_notice_description"]//text()').extract()
        description = " ".join(description).strip()

        address = response.xpath('.//div[@class="estate-explore-location"]//text()').extract_first()
        item_loader.add_value('address', address)

        addressList = address.split(", ")
        if len(addressList) <= 3:
            city = addressList[-2]
            zipcode = addressList[-1]
        else:
            city = addressList[-3]
            zipcode = addressList[-1]
        
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)

        javascript = response.xpath('.//script[contains(text(), "showMap")]/text()').extract_first()
        xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
        selector = Selector(text=xml)
        lat_lng = selector.xpath('.//funcdecl[@name="defaultLocation"]//arguments/number/@value').extract()
        if len(lat_lng) == 2:
            item_loader.add_value('latitude', lat_lng[0])
            item_loader.add_value('longitude', lat_lng[1])

        room_count_string = response.xpath('//span[text()="Bedrooms"]/following-sibling::text()').extract_first()
        if room_count_string:
            item_loader.add_value('room_count', room_count_string)
        bathroom_count_string = response.xpath('.//span[text()="Bathrooms"]/following-sibling::text()').extract_first()
        if bathroom_count_string:
            item_loader.add_value('bathroom_count', bathroom_count_string)

        features = response.xpath('.//ul[@class="amenties-list"]//text()').extract()
        if features:
            featuresString = " ".join(features)

            # https://www.g8property.co.uk/property/517-180-sheffield-road
            if "parking" in featuresString.lower(): 
                item_loader.add_value('parking', True)

            if "elevator" in featuresString.lower() or 'lift' in featuresString.lower(): 
                item_loader.add_value('elevator', True)

            if "balcony" in featuresString.lower(): 
                item_loader.add_value('balcony', True)

            # https://www.g8property.co.uk/property/551-6-bedroom-hmo
            if "terrace" in featuresString.lower(): 
                item_loader.add_value('terrace', True)

            if "swimming pool" in featuresString.lower():
                item_loader.add_value('swimming_pool', True)

            # https://www.g8property.co.uk/property/557-shared-houses-on-45-47-church-lane
            if "washing machine" in featuresString.lower():
                item_loader.add_value('washing_machine', True)

            # https://www.g8property.co.uk/property/478-hmo-property-for-rent
            if "dishwasher" in featuresString.lower() or "washer" in featuresString.lower():
                item_loader.add_value('dishwasher', True)
    
            # https://www.g8property.co.uk/property/478-hmo-property-for-rent
            if " furnished" in featuresString.lower(): 
                item_loader.add_value('furnished', True)
            elif "unfurnished" in featuresString.lower() or "un-furnished" in featuresString.lower(): 
                item_loader.add_value('furnished', False)

            if "pets considered" in featuresString.lower(): 
                item_loader.add_value('pets_allowed', True)

        min_max_rent=response.xpath('//input[@id="room_min_max_price"]/@value').extract_first()
        if "POA" not in min_max_rent:
            rent_list = min_max_rent.split(" - ")
            if len(rent_list) > 1:
                min_rent = rent_list[0]
                max_rent = rent_list[1].split(" ")[0]
                period = rent_list[1].split(" ")[1].lower()
                rent_string = min_rent
                description = f"{description} Note: Price of this property ranges from {min_rent} to {max_rent}."
            else:
                rent_string = rent_list[0].split(" ")[0]
                period = rent_list[0].split(" ")[1].lower()
            if period == "pw" or period == 'pppw':
                currency = rent_string[0]
                rent = int(rent_string.strip(currency))*4
                rent_string = currency+str(rent)
            item_loader.add_value('rent_string', rent_string)

        item_loader.add_value('description', description)

        images = response.xpath('.//div[@class="detail_slider_image"]/@style')
        images = [i.re_first(r'url\(\'(.*)\'\)') for i in images]
        item_loader.add_value('images', images)

        available_date = response.xpath('.//span[text()="Available from"]/following-sibling::text()').extract_first()
        if available_date:
            available_date = available_date.strip()
            available_date = format_date(available_date, '%d %b %Y')
            item_loader.add_value('available_date', available_date)

        item_loader.add_value('landlord_name', "G8 Lettings and Property Management")
        item_loader.add_value('landlord_email', "Info@g8property.co.uk")
        item_loader.add_value('landlord_phone', "01226 397665")

        item_loader.add_value("external_source", "G8property_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value("external_link", response.meta.get("request_url"))

        self.position += 1
        item_loader.add_value("position", self.position)
        yield item_loader.load_item()
