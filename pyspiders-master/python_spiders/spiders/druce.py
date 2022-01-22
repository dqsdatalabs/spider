# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader

def extract_city_zipcode(_address):
    city = _address.split(", ")[-2]
    zipcode = _address.split(", ")[-1]
    return city, zipcode

class DruceSpider(scrapy.Spider):
    name = "druce"
    allowed_domains = ["druce.com"]
    start_urls = (
        'http://www.druce.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = "https://www.druce.com/lettings/?display=100&view=list&type=lettings&location=&price_max=&bedrooms_min=&keywords="
        yield scrapy.Request( url=start_urls, callback=self.parse, dont_filter=True )

    def parse(self, response, **kwargs):
        links = response.xpath('//li[contains(@class, "property-item")]//h3/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details)
    
    def get_property_details(self, response):
        # parse details of the pro
        property_type = response.meta.get('property_type')
        external_link = response.url
        if 'house' in external_link.lower() or 'maison' in external_link.lower():
            property_type = 'house'
        elif 'flat' in external_link.lower() or 'apartment' in external_link.lower() or 'terrace' in external_link.lower():
            property_type = 'apartment'
        else:
            prop_type = "".join(response.xpath('//p[@class="property-description"]/text()').getall())
            if 'house' in prop_type.lower() or 'maison' in prop_type.lower():
                property_type = 'house'
            elif 'flat' in prop_type.lower() or 'apartment' in prop_type.lower() or 'terrace' in prop_type.lower():
                property_type = 'apartment'
        try:
            lat = re.search(r'lat     =\s(.*?)\,', response.text).group(1)
            lon = re.search(r'lng     =\s(.*?)\,', response.text).group(1) 
        except:
            lat = ''
            lon = ''
        key_values = response.xpath('//ul[@class="key-features"]/li')
        floor_plan_img = response.xpath('//div[@id="floorplan-tab"]/p/a/@href').extract()
        room_count = ''.join(response.xpath('//li[@class="bedrooms"]/text()').extract()).split(' x ')[-1]
        # if property_type and room_count: 
        address = response.xpath('//h1[@class="title-primary"]/text()').extract_first()
        city, zipcode = extract_city_zipcode(address)
        bathrooms = ''.join(response.xpath('//li[@class="bathrooms"]/text()').extract()).split(' x ')[-1]
        rent_string = response.xpath('//span[@class="price"]/text()').extract_first('').strip()
        rent_value = re.findall(r'([\d|,|\.]+)', rent_string)[0].replace(',', '')
        rent_month = str(int(rent_value) * 4) + '£'
        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_link.split("-to-rent/")[1].split("/")[0])
        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_xpath('description', '//p[@class="property-description"]/text()')
        item_loader.add_value('rent_string', rent_month)
        if lat:
            if not lat == "0.000000":
                item_loader.add_value('latitude', str(lat))
        if lon:
            if not lon == "0.000000":
                item_loader.add_value('longitude', str(lon))
        item_loader.add_xpath('images', '//div[@class="fotorama"]//img/@src')
        item_loader.add_value('floor_plan_images', floor_plan_img)
        item_loader.add_value('room_count', str(room_count))
        if bathrooms: 
            item_loader.add_value('bathroom_count', str(bathrooms))
        item_loader.add_xpath('title', '//title/text()')
        item_loader.add_value('landlord_name', 'Druce & Co')
        item_loader.add_value('landlord_email', 'w1sales@druce.com')
        item_loader.add_value('landlord_phone', '+44 (0) 20 7935 6535')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))

        furnished = response.xpath("//li[@class='furnishings']/text()").get()
        if furnished:
            if furnished.lower().strip() == 'unfurnished': item_loader.add_value("furnished", False)
            elif furnished.lower().strip() == 'furnished': item_loader.add_value("furnished", True)
        

        yield item_loader.load_item() 
