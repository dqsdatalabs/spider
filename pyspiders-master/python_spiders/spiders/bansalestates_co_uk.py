# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import js2xml
import lxml
import scrapy
from scrapy import Selector

from ..helper import format_date
from ..loaders import ListingLoader


class GiraffeletsPyspiderUnitedkingdomEnSpider(scrapy.Spider):
    name = 'bansalestates_co_uk'
    allowed_domains = ['bansalestates.co.uk']
    start_urls = ['https://www.bansalestates.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = [
            {
                'url': 'https://www.bansalestates.co.uk/properties/lettings/tag-apartment/status-available',
                'property_type': 'apartment'},
            {
                'url': 'https://www.bansalestates.co.uk/properties/lettings/tag-flat/status-available',
                'property_type': 'apartment'},
            {
                'url': 'https://www.bansalestates.co.uk/properties/lettings/tag-house/status-available',
                'property_type': 'house'},
            {
                'url': 'https://www.bansalestates.co.uk/properties/lettings/tag-studio/status-available',
                'property_type': 'studio'},
            {
                'url': 'https://www.bansalestates.co.uk/properties/lettings/tag-bungalows/status-available',
                'property_type': 'house'},
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get("url"),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[contains(@class,"extra")]')
        if listings:
            for property_item in listings:
                property_url = property_item.xpath('.//a/@href').extract_first()
                bed_bath = property_item.xpath('.//h6/text()').extract()
                yield scrapy.Request(
                    url=f"https://www.bansalestates.co.uk{property_url}",
                    callback=self.get_property_details,
                    meta={'request_url': f"https://www.bansalestates.co.uk{property_url}",
                          'property_type': response.meta.get('property_type'),
                          "bed": bed_bath[0],
                          "bathroom": bed_bath[1]
                          }
                )
        next_page_url = response.xpath('.//a[@class="pagination_next"]/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(
                url=response.urljoin(f"https://www.bansalestates.co.uk{next_page_url}"),
                callback=self.parse,
                meta={'request_url': f"https://www.bansalestates.co.uk{next_page_url}",
                      'property_type': response.meta.get('property_type')}
            )

    def get_property_details(self, response):
        features = " ".join(response.xpath('.//ul[@id="points"]/li/text()').extract())
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_value('external_link', response.meta.get('request_url'))
        # set refernce id from url because some ref id are missing in page
        item_loader.add_value('external_id', response.meta.get('request_url').split("/")[-2])
        item_loader.add_xpath('rent_string', './/h3[@id="propertyPrice"]/text()')
        item_loader.add_xpath('title', './/title/text()')
        item_loader.add_xpath('address', './/h2[@id="secondary-address"]/text()')
        item_loader.add_xpath('description', './/div[@class="section box"]//p/text()')
        item_loader.add_xpath('images', './/div[@id="propertyDetailPhotos"]//img/@src')

        item_loader.add_value('landlord_name', 'Bansale States')
        item_loader.add_value('landlord_email', 'sales@bansalestates.co.uk')
        item_loader.add_value('landlord_phone', '02476 231132')

        javascript = response.xpath('.//script[contains(text(),"LatLng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            latitude = xml_selector.xpath('.//identifier[@name="LatLng"]/../../../arguments/number/@value').extract()[0]
            longitude = xml_selector.xpath('.//identifier[@name="LatLng"]/../../../arguments/number/@value').extract()[1]
            item_loader.add_value('latitude', latitude)
            item_loader.add_value('longitude', longitude)

        item_loader.add_value('room_count', response.meta.get('bed'))
        item_loader.add_value('bathroom_count', response.meta.get('bathroom'))

        date = response.xpath(
            './/strong[contains(text(),"Property available on:")]/following::text()').extract_first()
        if date:
            item_loader.add_value('available_date', format_date(date.strip()))
        if item_loader.get_output_value('address'):
            city_zip = item_loader.get_output_value('address').split(', ')[-1].split(" ")
            # used this expression because the the city and zip can be two words or single and there is no particular separator
            zipcode = ""
            city = ""
            for text in city_zip:
                if text.isalpha():
                    city += f" {text}"
                else:
                    zipcode += f" {text}"

            item_loader.add_value('zipcode', zipcode.strip())
            item_loader.add_value('city', city.strip())

        # these fields are present in key li tags
        # EX https://www.bansalestates.co.uk/properties/8563848/lettings
        if "PARKING" in features:
            item_loader.add_value('parking', True)
        # ex https://www.bansalestates.co.uk/properties/9530379/lettings
        if "FURNISHED" in features:
            item_loader.add_value('furnished', True)

        # https://www.bansalestates.co.uk/properties/12776488/lettings
        if "WASHING MACHINE" in features:
            item_loader.add_value('washing_machine', True)

        if "PARKING" in features:
            item_loader.add_value('parking', True)

        if "ELEVATOR" in features or "LIFT" in features:
            item_loader.add_value('elevator', True)

        if "BALCONY" in features:
            item_loader.add_value('balcony', True)

        if "TERRACE" in features:
            item_loader.add_value('terrace', True)

        if "SWIMMING POOL" in features:
            item_loader.add_value('swimming_pool', True)

        if "DISHWASHER" in features:
            item_loader.add_value('dishwasher', True)

        utilities = response.xpath("//div[contains(@class,'title')]//p//text()[contains(.,'included') and contains(.,'£')]").get()
        if utilities:
            utilities = utilities.split("£")[1].split(".")[0]
            item_loader.add_value("utilities", utilities)

        self.position += 1
        item_loader.add_value('position', self.position)

        item_loader.add_value("external_source",
                              "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item()
