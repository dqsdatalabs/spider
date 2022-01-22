# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader

def extract_city_zipcode(_address):
    city = _address.split(", ")[-2].strip()
    zipcode = _address.split(", ")[-1].strip()
    return city, zipcode

class HwestateagentsSpider(scrapy.Spider):
    name = "hwestateagents"
    allowed_domains = ["hwestateagents.co.uk"]
    start_urls = (
        'http://www.hwestateagents.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.hwestateagents.co.uk/?ct_state&ct_price_from&ct_price_to&ct_property_type=apartment&ct_beds&ct_additional_features=Lettings&ct_ct_status=available&search-listings=true', 'property_type': 'apartment'},
            {'url': 'https://www.hwestateagents.co.uk/?ct_state&ct_price_from&ct_price_to&ct_property_type=studio&ct_beds&ct_additional_features=Lettings&ct_ct_status=available&search-listings=true', 'property_type': 'studio'},
            {'url': 'https://www.hwestateagents.co.uk/?ct_state&ct_price_from&ct_price_to&ct_property_type=semi-detached-house&ct_beds&ct_additional_features=Lettings&ct_ct_status=available&search-listings=true', 'property_type': 'house'},
            {'url': 'https://www.hwestateagents.co.uk/?ct_state&ct_price_from&ct_price_to&ct_property_type=terraced-house&ct_beds&ct_additional_features=Lettings&ct_ct_status=available&search-listings=true', 'property_type': 'apartment'},
            {'url': 'https://www.hwestateagents.co.uk/?ct_state&ct_price_from&ct_price_to&ct_property_type=end-terraced-house&ct_beds&ct_additional_features=Lettings&ct_ct_status=available&search-listings=true', 'property_type': 'apartment'},
            {'url': 'https://www.hwestateagents.co.uk/?ct_state&ct_price_from&ct_price_to&ct_property_type=maisonette&ct_beds&ct_additional_features=Lettings&ct_ct_status=available&search-listings=true', 'property_type': 'house'},
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//a[@id="more-details-btn"]')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        if response.xpath('//a[contains(text(), "Next Page")]/@href'):
            next_link = response.xpath('//a[contains(text(), "Next Page")]/@href').extract_first()
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        # parse details of the propery
        property_type = response.meta.get('property_type')
        external_link = response.url
        address = response.xpath('//h1[@id="listing-title"]/text()').extract_first()
        city, zipcode = extract_city_zipcode(address) 
        room_count_text = response.xpath('//span[contains(text(), "bedroom")]/text()').extract_first('').strip()
        if room_count_text:
            try:
                room_count = re.findall(r'\d+', room_count_text)[0]    
            except:
                room_count = ''
        else:
            room_count = ''
        rent_string = response.xpath('//span[@class="listing-price"]/text()').extract_first('').strip()
        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        id=response.xpath("//link[@rel='shortlink']/@href").get()
        if id:
            id=id.split("=")[-1]
            item_loader.add_value("external_id",id)

        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
    
        description = response.xpath('//div[@id="listing-content"]/p/text()').get()
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()))
    
        item_loader.add_value('rent_string', rent_string)
        images = [x for x in response.xpath('//ul[@class="slides"]//img/@src | //figure[contains(@id,"first-image")]/img/@src').getall()]
        item_loader.add_value("images", images)
        if room_count:
            item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('landlord_name', 'HW Estate Agents')
        item_loader.add_value('landlord_email', 'info@hwestateagents.co.uk')
        item_loader.add_value('landlord_phone', ' 01273 359000')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item() 