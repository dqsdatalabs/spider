# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import js2xml
import lxml
import scrapy
from scrapy import Selector, FormRequest
from ..helper import extract_number_only, extract_rent_currency
from ..loaders import ListingLoader
from ..user_agents import random_user_agent


class DanversestatesSpider(scrapy.Spider):
    name = 'danversestates_com'
    allowed_domains = ['danversestates.com']
    start_urls = ['https://www.danversestates.com/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = ["https://www.danversestates.com/lettings.php",
                      "https://www.danversestates.com/students.php"]
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//a[contains(@class, "info_hover_property")]/@href').extract()
        listings = [response.urljoin(l_i) for l_i in listings]
        for property_url in listings:
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'request_url': property_url}
            )
        if len(response.xpath('.//li[@class="active"]/following-sibling::li[1]//a/@title').extract()) == 0:
            next_page_url = response.xpath('.//li[@class="active"]/following-sibling::li[1]//a/@href').extract_first()
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={"request_url": next_page_url}
            )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        address = response.xpath('.//title/text()').extract_first()

        property_status = response.xpath('.//li[contains(text(), "Property Status")]/following-sibling::li[1]/text()').extract_first()
        if property_status and property_status.lower() == "let agreed":
            return

        # property_type
        property_type = response.xpath('.//li[contains(text(), "Property Type")]/following-sibling::li[1]/text()').extract_first()
        if property_type and property_type.lower() == "garage":
            return
        elif property_type:
            property_type = property_type.lower()
            property_type_mapping = {"flat": "apartment", "house": "house"}
            item_loader.add_value('property_type', property_type_mapping[property_type])

        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_xpath('external_id', './/li[contains(text(),"Reference")]/following::li/text()')
        item_loader.add_xpath('title', './/h1[@class="page-header"]/text()')
        item_loader.add_value('address', address)

        # rent_string
        rent_string = response.xpath('.//div[@class="column"]//h2/text()').extract_first()
        if rent_string and "pw" in rent_string.lower():
            rent = "".join(filter(str.isnumeric, rent_string.replace(',', '')))
            item_loader.add_value("rent", str(int(float(rent)*4))) 
            item_loader.add_value('currency', "GBP")
        elif rent_string:
            item_loader.add_value('rent_string', rent_string)

        item_loader.add_xpath('description', './/div[@class="property-detail"]/text()')
        item_loader.add_xpath('bathroom_count', './/li[.="Bathrooms"]/following-sibling::li[1]/text()')
        item_loader.add_xpath('room_count', '//li[.="Bedrooms"]/following-sibling::li[1]/text()')
        item_loader.add_xpath('images', './/div[@class="ws_images"]//img/@src')
        item_loader.add_value('landlord_name', 'Danvers Estates')
        item_loader.add_value('landlord_email', 'info@danversestates.com')
        item_loader.add_value('landlord_phone', '0116 275 8888')

        javascript = response.xpath('.//script[contains(text(),"LatLng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            latitude = xml_selector.xpath('.//identifier[@name="LatLng"]/../../../arguments/number/@value').extract()[0]
            longitude = xml_selector.xpath('.//identifier[@name="LatLng"]/../../../arguments/number/@value').extract()[1]

            if latitude != '0' and longitude != '0':
                item_loader.add_value('latitude', latitude)
                item_loader.add_value('longitude', longitude)

        # based on the data only this seems to be required
        item_loader.add_value('city', address.split(",")[-2])
        item_loader.add_value('zipcode', address.split(",")[-1])

        if len(response.xpath('.//li[contains(text(), "Balconies") or contains(text(), "Balcony")]')) > 0:
            item_loader.add_value('balcony', True)
        if len(response.xpath('.//li[contains(text(), "Furnished")]')) > 0:
            item_loader.add_value('furnished', True)

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Danversestates_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
