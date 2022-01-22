# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
import re
from ..loaders import ListingLoader

class FreshpropertyhubSpider(scrapy.Spider):
    name = "freshpropertyhub"
    allowed_domains = ["freshpropertyhub.co.uk"]
    start_urls = (
        'http://www.freshpropertyhub.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://freshpropertyhub.co.uk/lettings/?sort=price&minbed=0&maxbed=1000&minprice=0&maxprice=1000000000000&postcode=all-postcodes&prop-type=FlatApartment', 'property_type': 'apartment'},
            {'url': 'https://freshpropertyhub.co.uk/lettings/?sort=price&minbed=0&maxbed=1000&minprice=0&maxprice=1000000000000&postcode=all-postcodes&prop-type=House', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//a[contains(text(), "View Property")]')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        # parse details of the propery
        property_type = response.meta.get('property_type')
        external_link = response.url
        street = response.xpath('//div[@class="address"]/h2/text()').extract_first()
        city_zip = response.xpath('//div[@class="address"]/h3/text()').extract_first()
        city = city_zip.split(', ')[0]
        zipcode = city_zip.split(', ')[1]  
        address = street + ' ' + city_zip
        room_count = response.xpath('//div[@class="bed"]//div[@class="count"]/p/text()').extract_first('').strip()
        rent_string = response.xpath('//div[@class="price"]/h5/text()').extract_first('').strip()
        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_xpath('description', '//div[@class="property-description"]//text()')
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_xpath('images', '//div[@class="images-slide"]/img/@src')
        if room_count:
            item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('landlord_name', 'Fresh Estate and Letting Agents')
        item_loader.add_value('landlord_email', 'sales@freshsales.co.uk')
        item_loader.add_value('landlord_phone', '01792 464757')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item() 