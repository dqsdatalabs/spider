# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
import re
from ..loaders import ListingLoader

class VillageEstatesSpider(scrapy.Spider):
    name = "village_estates"
    allowed_domains = ["village-estates.com"]
    start_urls = (
        'http://www.village-estates.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = "https://www.village-estates.com/properties-to-let"
        yield scrapy.Request( url=start_urls, callback=self.parse, dont_filter=True )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[@class="property-thumb-container"]/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True )
        if response.xpath('//a[@title="Next"]/@href'):
            next_link = response.urljoin(response.xpath('//a[@title="Next"]/@href').extract_first())
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True)
        
    def get_property_details(self, response):
         # parse details of the pro
        street = response.xpath('//address/strong/text()').extract_first('')
        city_zipcode = response.xpath('//address/text()').extract_first('')
        address = street + ' ' + city_zipcode
        description_li = ''.join(response.xpath('//ul[@id="starItem"]/li/text()').extract())
        description_p = ''.join(response.xpath('//div[@id="propdescription"]//p//text()').extract()) 
        description = description_li + description_p  
        if 'apartment' in description.lower():
            property_type = 'apartment'
        elif 'flat' in description.lower():
            property_type = 'apartment'
        else:
            property_type = 'house'
        external_link = response.url
        external_id = response.xpath('//b[contains(text(), "Ref")]/following-sibling::text()').extract_first()
        title = response.xpath('//title/text()').extract_first()
        room_count = response.xpath('//i[contains(@class,"bed")]/following-sibling::strong/text()').extract_first('').strip()
        bathrooms = response.xpath('//i[contains(@class,"bath")]/following-sibling::strong/text()').extract_first('').strip() 
        try:
            lat = re.search(r'lat\:\s\"(.*?)\"', response.text).group(1)
        except:
            lat = ''
        try:
            lon = re.search(r'lon\:\s\"(.*?)\"', response.text).group(1)
        except:
            lon = ''
        rent_string = response.xpath('//small[@class="eapow-detail-price"]/text()').extract_first('').strip()
        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('title', title)
        item_loader.add_value('address', address)
        if city_zipcode:
            city = city_zipcode.split(" ")[0]
            zipcode = city_zipcode.split(city)[1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        item_loader.add_value('description', description)
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_xpath('images', '//ul[@class="slides"]//@data-src')
        if room_count:
            item_loader.add_value('room_count', str(room_count))
        if lat:
            item_loader.add_value('latitude', str(lat))
        if lon:
            item_loader.add_value('longitude', str(lon))
        if bathrooms: 
            item_loader.add_value('bathroom_count', str(bathrooms))
        item_loader.add_value('landlord_name', 'Village Estates (Sidcup) Ltd')
        item_loader.add_value('landlord_email', 'bexley@village-estates.com')
        item_loader.add_value('landlord_phone', '01322 522111')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        
        status = response.xpath("//div[b[contains(.,'Sale Type')]]/text()").get()
        if status and "for rent" in status.lower():
            yield item_loader.load_item() 