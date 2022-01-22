# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy

from ..helper import extract_rent_currency, extract_number_only
from ..loaders import ListingLoader


class PropertybynestSpider(scrapy.Spider):
    name = 'propertybynest_co_uk'
    allowed_domains = ['propertybynest.co.uk']
    start_urls = ['https://www.propertybynest.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = [
            {
                'url': 'https://www.propertybynest.co.uk/search/',
                'property_type': 'studio'}]
        for url in start_urls:
            yield scrapy.Request(url=url.get("url"),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//a[contains(@class,"box_plain")]/@href').extract()
        for property_item in listings:
            yield scrapy.Request(
                url=property_item,
                callback=self.get_property_details,
                meta={'request_url': property_item,
                      'property_type': response.meta.get('property_type')}
            )

    def get_property_details(self, response):
        rent = response.xpath('.//div[@class="each-year"]//td[contains(text(),"£")]/text()').extract_first()
        rent_string = response.xpath('.//div[@class="each-year"]//td[contains(text(),"£")]/text()').extract_first()
        max_rent = max(rent_string) * 4
        min_rent = min(rent_string) * 4
        description = response.xpath('.//div[contains(@class,"column_twothirds")]//p/text()').extract_first()
        description = description + f". Note: price for this property range from £{min_rent} to £{max_rent}"
        img = [image.split("url(")[-1].split(")")[0] for image in
               response.xpath('.//div[@class="carousel_single"]//@data-bg').extract()]

        map = response.xpath('.//div[contains(@class,"map_canvas")]').extract_first()
        parking = response.xpath('.// h4 / strong[contains(text(), "parking")]').extract_first()

        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', response.meta.get('request_url').strip("/").split("/")[-1])
        item_loader.add_xpath('title', './/h1/text()')
        item_loader.add_xpath('address', './/p[@class="center"]/text()')
        item_loader.add_value('rent_string', f"£{int(float(rent.replace('£','')))*4}")


        # square_meters
        square_meters = response.xpath('.//div[@id="book_now"]//sup[contains(text(), "2")]/../text()[1]').extract()
        square_meters.extend(response.xpath('.//div[@id="book_now"]//li[contains(text(), "m²")]/text()').extract())
        square_meters = [extract_number_only(s_i) for s_i in square_meters]
        if len(square_meters) > 0:
            item_loader.add_value('square_meters', min(square_meters))

        item_loader.add_value('description', description)
        item_loader.add_value('images', img)
        item_loader.add_value('property_type', response.meta.get('property_type'))
        if response.meta.get('property_type') == "studio":
            item_loader.add_value('room_count', 1)
        item_loader.add_xpath('floor_plan_images', './/a[@data-lightbox="floorplans"]/@href')
        item_loader.add_value('landlord_name', 'Property By Nest')
        item_loader.add_value('landlord_email', 'info@propertybynest.co.uk')
        item_loader.add_value('landlord_phone', '+44 (0)191 640 3450')

        if map:
            item_loader.add_xpath('latitude', './/div[contains(@class,"map_canvas")]/@data-lat')
            item_loader.add_xpath('longitude', './/div[contains(@class,"map_canvas")]/@data-long')

        if item_loader.get_output_value('address'):
            city_zip = item_loader.get_output_value('address').split(', ')
            item_loader.add_value('city', city_zip[-2])
            item_loader.add_value('zipcode', city_zip[-1])
        # https://www.propertybynest.co.uk/properties/tyne-bridge-apartments/
        if parking:
            item_loader.add_value('parking', True)

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Propertybynest_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
