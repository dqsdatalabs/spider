# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
import re
from ..loaders import ListingLoader

class LudlowthompsonSpider(scrapy.Spider):
    name = "ludlowthompson"
    allowed_domains = ["www.ludlowthompson.com"]
    start_urls = (
        'http://www.www.ludlowthompson.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.ludlowthompson.com/search/search.htm?clean=Yes&type=full&order=price_desc&tab=area&changeDivision=2&tmp_price_min=0&price_min=&tmp_price_max=&price_max=&apartments=t&bedrooms_min=&bedrooms_max=&submit_button=Search', 'property_type': 'apartment'},
            {'url': 'https://www.ludlowthompson.com/search/search.htm?clean=Yes&type=full&order=price_desc&tab=area&changeDivision=2&tmp_price_min=0&price_min=&tmp_price_max=&price_max=&houses=t&bedrooms_min=&bedrooms_max=&submit_button=Search', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//li[@class="jsPropertyResult"]')
        for link in links: 
            url = response.urljoin(link.xpath('./@data-url').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        if response.xpath('//div[@id="jsAdminFeeModal"]/preceding-sibling::div[1]//a[@class="next"]/@href'):
            next_link = response.urljoin(response.xpath('//div[@id="jsAdminFeeModal"]/preceding-sibling::div[1]//a[@class="next"]/@href').extract_first())
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    
    def get_property_details(self, response):
        #parse detail information
        property_type = response.meta.get('property_type')
        external_link = response.url
        external_id = response.xpath('//div[@id="interestedCont"]//strong[contains(text(), "REF")]/text()').extract_first('').strip().split(': ')[-1]
        address = response.xpath('//div[contains(@class, "propertyDetails")]//h2/text()').extract_first('').strip()
        city_zipcode = address.split(' - ')[-1]
        zipcode = city_zipcode.split(', ')[-1]
        city = city_zipcode.split(', ')[0]   
        title = response.xpath('//div[contains(@class, "propertyDetails")]//h1/text()').extract_first('').strip().replace('\xa0', '') 
        room_count = title.split(', ')[0].split(' ')[0]
        bathrooms = response.xpath('//div[contains(@class, "propertyDetails")]//li[@class="bathrooms"]/text()').extract_first('').strip() 
        rent_string = response.xpath('//div[@id="price"]/div[@class="priceNavInner"]/text()').extract_first('').strip()
        rent_value = re.findall(r'([\d|,|\.]+)', rent_string)[0].replace(',', '')
        rent_month = str(int(rent_value) * 4) + '£'  
        landlord_phone = response.xpath('//div[@id="interestedCont"]//p[@class="phone"]/a/text()').extract_first('').strip()
        lat = response.xpath('//div[@class="similarPropertiesList"]//li[@class="jsPropertyResult"]/@data-lat').extract_first('').strip()
        lng = response.xpath('//div[@class="similarPropertiesList"]//li[@class="jsPropertyResult"]/@data-lng').extract_first('').strip()
        description = ''.join(response.xpath('//div[contains(@class, "propertyDetails")]//p//text()').extract())
        features = response.xpath('//ul[@class="bulletCopy"]/li')
        balcony = ''
        floor = ''
        for fea in features:
            dec = fea.xpath('.//text()').extract_first('')
            if 'balcony' in dec.lower():
                balcony = True
            if 'floor' in dec.lower():
                try:
                    floor = re.findall(r'\d+', dec)[0]
                    floor = str(floor)
                except:
                    floor = ''
        if not landlord_phone:
            landlord_phone = '020 7480 0120'     
        floor_plan_images = response.urljoin(response.xpath('//a[contains(@href, "floorplan")][@class="show-for-small"]/@href').extract_first(''))
        item_loader = ListingLoader(response=response)
        property_type = response.meta.get('property_type')
        item_loader.add_value('external_id', str(external_id))
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('title', title)
        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        if 'swimming pool' in description.lower():
            item_loader.add_value('swimming_pool', True)
        if 'washing machine' in description.lower():
            item_loader.add_value('washing_machine', True)
        if 'furnished' in description.lower():
            item_loader.add_value('furnished', True)
        if balcony:
            item_loader.add_value('balcony', True)  
        if floor:
            item_loader.add_value('floor', floor) 
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_xpath('description', '//div[contains(@class, "propertyDetails")]//p//text()')
        item_loader.add_value('rent_string', rent_month)
        item_loader.add_xpath('images', '//img[@id="photoMain"]/@src')
        item_loader.add_value('floor_plan_images', floor_plan_images)
        item_loader.add_value('latitude', str(lat))
        item_loader.add_value('longitude', str(lng))
        item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('bathroom_count', str(bathrooms))
        item_loader.add_value('landlord_name', 'Ludlow Thompson')
        item_loader.add_value('landlord_email', 'recruitment@ludlowthompson.com')
        item_loader.add_value('landlord_phone', landlord_phone)
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item()