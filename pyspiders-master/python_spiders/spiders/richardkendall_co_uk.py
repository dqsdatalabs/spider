# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
import re
from ..helper import extract_number_only
import lxml
import js2xml
from scrapy import Selector


class RichardkendallSpider(scrapy.Spider):
    name = "richardkendall_co_uk"
    allowed_domains = ["www.richardkendall.co.uk"]
    start_urls = ( 
        'http://www.www.richardkendall.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):

        start_urls = [
            {'url': 'https://www.richardkendall.co.uk/wp-content/themes/richard-kendall-theme/ajax-load-properties.php?townOrPostCode=&search-type=For&20Sale&property-type=House&bedrooms=0&locations=undefined&minprice=0&maxprice=all&page=1&orderd=&action=footersearch&searchtype=refine&pageurl=https+$$www*richardkendall*co*uk$properties$to-let$?townOrPostCode=&property-type=House&bedrooms=0&minprice=0&maxprice=all&townOrPostCode=&orderd=&action=footersearch',
             'property_type': 'house'},
            {'url': 'https://www.richardkendall.co.uk/wp-content/themes/richard-kendall-theme/ajax-load-properties.php?townOrPostCode=&search-type=For&20Sale&property-type=Flat&bedrooms=0&locations=undefined&minprice=0&maxprice=all&page=1&orderd=&action=footersearch&searchtype=refine&pageurl=https+$$www*richardkendall*co*uk$properties$to-let$?townOrPostCode=&property-type=Flat&bedrooms=0&minprice=0&maxprice=all&townOrPostCode=&orderd=&action=footersearch',
             'property_type': 'apartment'},
            {'url': 'https://www.richardkendall.co.uk/wp-content/themes/richard-kendall-theme/ajax-load-properties.php?townOrPostCode=&search-type=For&20Sale&property-type=Bungalow&bedrooms=0&locations=undefined&minprice=0&maxprice=all&page=1&orderd=&action=footersearch&searchtype=refine&pageurl=https+$$www*richardkendall*co*uk$properties$to-let$?townOrPostCode=&property-type=Bungalow&bedrooms=0&minprice=0&maxprice=all&townOrPostCode=&orderd=&action=footersearch',
             'property_type': 'house'}]

        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'), 
                callback=self.parse,
                meta={'request_url': url.get('url'),
                      'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):

        listings = response.xpath('.//div[contains(@class,"full-details")]')
        for listing in listings:
            if not listing.xpath('.//span[contains(text(),"LET AGREED")]').extract_first():
                property_url = response.urljoin(listing.xpath('.//div[contains(@class,"title")]/a/@href').extract_first())
                room_count = listing.xpath('.//p[@class="bedrooms"]/span/text()').extract_first()
                bathroom_count = listing.xpath('.//p[@class="bathrooms"]/span/text()').extract_first()
                town_zip = listing.xpath('.//p[@class="town-postcode"]/text()').extract_first()
                yield scrapy.Request(
                    url=property_url, 
                    callback=self.get_property_details, 
                    meta={'request_url': property_url,
                          'property_type': response.meta.get('property_type'),
                          'room_count': room_count,
                          'bathroom_count': bathroom_count,
                          'town_zip': town_zip})

        if len(listings) == 12:
            current_page = re.findall(r"(?<=page=)\d+", response.meta["request_url"])[0]
            next_page_url = re.sub(r"(?<=page=)\d+", str(int(current_page) + 1), response.meta["request_url"])

            yield scrapy.Request(
                    url=next_page_url, 
                    callback=self.parse, 
                    meta={'request_url': next_page_url,
                          'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.meta.get('request_url'))
        externalid=response.xpath("//link[@rel='shortlink']/@href").get()
        if externalid:
            item_loader.add_value("external_id",externalid.split("p=")[-1])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("room_count", extract_number_only(response.meta.get('room_count')))
        item_loader.add_value("bathroom_count", extract_number_only(response.meta.get('bathroom_count')))
        rent_string = response.xpath('//p[@class="price"]/text()[normalize-space()]').extract_first()
        item_loader.add_value("rent_string", rent_string)
        item_loader.add_value("external_source", "Richardkendall_PySpider_{}_{}".format(self.country, self.locale))

        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        if response.meta.get('town_zip'):            
            item_loader.add_value('address', item_loader.get_output_value('title')+', '+response.meta.get('town_zip'))
            if ", " in response.meta.get('town_zip'):
                item_loader.add_value('city', response.meta.get('town_zip').split(', ')[-2])
                item_loader.add_value('zipcode', response.meta.get('town_zip').split(', ')[-1].strip())
            elif ". " in response.meta.get('town_zip'):
                item_loader.add_value('city', response.meta.get('town_zip').split('. ')[-2])
                item_loader.add_value('zipcode', response.meta.get('town_zip').split('. ')[-1].strip())

        description = response.xpath('//div[contains(@class,"resp-tabs-container")]/div[1]//text()').get()
        if description:
            item_loader.add_value('description', re.sub('\s{2,}', ' ', description))

        latlng = response.xpath('.//iframe[contains(@src,"maps")]/@src').extract_first()
        if latlng:
            latlng = re.search(r'(?<=\?q=).+?(?=&key=)', latlng).group().split(',')
            item_loader.add_value('latitude', latlng[0])
            item_loader.add_value('longitude', latlng[1])

        # to avoid incorrect links from the loader
        images = response.xpath("//div//noscript/img/@src").getall()
        for image in images:
            if "media2" in image:
                item_loader.add_value('images', image.strip())
        floor_plan_images = response.xpath('.//img[@alt="Floorplan"]/@src').extract()
        for image in floor_plan_images:
            item_loader.add_value('floor_plan_images', image.strip())

        item_loader.add_value('landlord_name', 'Richard Kendall')
        item_loader.add_value('landlord_phone', '01924 291 294')
        item_loader.add_value('landlord_email', 'mail@richardkendall.co.uk')

        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
