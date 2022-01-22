# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import format_date
 

class HaslamsSpider(scrapy.Spider):
    name = "haslams_net"
    allowed_domains = ['haslams.net']
    start_urls = ['https://haslams.net/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    # custom_settings = {
    #     "FEED_EXPORT_ENCODING" : "utf-8",
    #     "RETRY_HTTP_CODES": [500, 503, 504, 400, 401, 403, 405, 407, 408, 416, 456, 502, 429, 307],
    # }

    def start_requests(self):
        start_url = ["https://haslams.net/renting/find-homes-to-rent/"]
        for url in start_url:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url})
    
    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@class="post--property-list"]')
        for listing in listings:
            property_url = listing.xpath('.//a[contains(text(), "More details")]/@href').extract_first()
            room_count = listing.xpath('.//span[contains(text(), "Bedroom")]/../text()').extract_first()
            bathroom_count = listing.xpath('.//span[contains(text(), "Bathroom")]/../text()').extract_first()
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'request_url': property_url,
                      'room_count': room_count,
                      'bathroom_count': bathroom_count}
            )

        next_page_url = response.xpath('.//a[@class="pagination__next"]/@href').extract_first()
        if next_page_url:
            next_page_url = response.urljoin(next_page_url)
            yield scrapy.Request(url= next_page_url,
                                 callback=self.parse,
                                 meta={'request_url': next_page_url})

    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_xpath('external_id', './/input[@id="make-an-offer-propertyref"]/@value')
        item_loader.add_xpath('images', './/img[contains(@alt, "Thumbnail Image")]/@src')

        address = response.xpath('//p[contains(@class,"h1")]/text()').extract_first()
        item_loader.add_value('address', address)
        item_loader.add_value('title', address)
        if address:
            item_loader.add_value('city', address.split(', ')[1])
        if address:
            item_loader.add_value('zipcode', address.split(', ')[-1])
        item_loader.add_xpath('latitude', '//div[@class="gmap"]/div/@data-lat')
        item_loader.add_xpath('longitude', '//div[@class="gmap"]/div/@data-lng')
        item_loader.add_xpath('rent_string', './/h2[contains(@class,"prop-price--half")]/text()')

        available_date = response.xpath('//span[@class="av-from"]/text()').extract_first()
        if available_date:
            available_date=available_date.replace('Available from: ','')
            if not "now" in available_date.lower():
                item_loader.add_value('available_date', format_date(available_date))

        if response.meta["room_count"]:
            item_loader.add_value('room_count', response.meta["room_count"])
        if response.meta["bathroom_count"]:
            item_loader.add_xpath('bathroom_count', response.meta["bathroom_count"])
        item_loader.add_xpath('description', '//span[contains(@class,"long-description")]/text()')

        if any(item in item_loader.get_output_value('description').lower() for item in ['studio', 'bedsit']):
            item_loader.add_value('property_type', 'studio')
        elif any(item in item_loader.get_output_value('description').lower() for item in ['apartment']):
            item_loader.add_value('property_type', 'apartment')
        elif any(item in item_loader.get_output_value('description').lower() for item in ['house']):
            item_loader.add_value('property_type', 'house')
        else:
            return
        
        # https://haslams.net/property/westcote-road-reading-rg30-2gt/
        furnished = response.xpath('//li[contains(text(), "urnished")]/text()').extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value('furnished', False)
            else:
                item_loader.add_value('furnished', True)

        # https://haslams.net/property/westcote-road-reading-rg30-2gt/
        parking = response.xpath('//li[contains(text(), "parking") or contains(text(), "Parking")]/text()').extract_first()
        if parking:
            item_loader.add_value('parking', True)

        swimming_pool = response.xpath('//li[contains(text(), "swimming_pool") or contains(text(), "Swimming Pool")]/text()').extract_first()
        if swimming_pool:
            item_loader.add_value('swimming_pool', True)

        elevator = response.xpath('//li[contains(text(), "elevator") or contains(text(), "Elevator")]/text()').extract_first()
        if elevator:
            item_loader.add_value('elevator', True)

        terrace = response.xpath('//li[contains(text(), "terrace") or contains(text(), "Terrace")]/text()').extract_first()
        if terrace:
            item_loader.add_value('terrace', True)

        balcony = response.xpath('//li[contains(text(), "balcony") or contains(text(), "Balcony")]/text()').extract_first()
        if balcony:
            item_loader.add_value('balcony', True)

        item_loader.add_xpath('landlord_phone', './/p[contains(@class,"content-box")]/a[contains(@href,"tel")]/text()')
        item_loader.add_value('landlord_email', 'enquiries@haslams.net')
        item_loader.add_value('landlord_name','Haslams Estate Agents Ltd')
        
        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Haslams_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
