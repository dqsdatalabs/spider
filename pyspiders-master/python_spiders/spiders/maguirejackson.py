# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader

class MaguirejacksonSpider(scrapy.Spider):
    name = "maguirejackson"
    allowed_domains = ["www.maguirejackson.com"]
    start_urls = (
        'http://www.maguirejackson.com/',
    ) 
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.maguirejackson.com/search/list?limit=48&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_division=residential&p_department=RL&propertyAge=&national=false&location=&propertyType=9%2C28%2C29&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=', 'property_type': 'apartment'},
            {'url': 'https://www.maguirejackson.com/search/list?limit=48&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_division=residential&p_department=RL&propertyAge=&national=false&location=&propertyType=22&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[contains(@class, "search-results-list-property")]//a[contains(text(), "Details")]')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        if response.xpath('//ul[@class="pagination"]//a[contains(text(), "›")]/@href'):
            next_link = response.urljoin(response.xpath('//ul[@class="pagination"]//a[contains(text(), "›")]/@href').extract_first())
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    
    def get_property_details(self, response):
        title = response.xpath('//h3[contains(text(), "Overview")]/following-sibling::h1/text()').extract_first()
        if 'house' in title.lower():
            property_type = 'house'
        elif 'apartment' in title.lower():
            property_type = 'apartment'
        elif 'flat' in title.lower():
            property_type = 'apartment' 
        else:
            property_type = ''
        if property_type:
            external_link = response.url
            address = title.split(' in ')[-1]
            room_count_text = response.xpath('//img[contains(@src, "bedroom")]/following-sibling::span/text()').extract_first('').strip()
            if room_count_text:
                room_count = re.findall(r'\d+', room_count_text)[0]     
            bathrooms_text = response.xpath('//img[contains(@src, "bathroom")]/following-sibling::span/text()').extract_first('').strip() 
            if bathrooms_text:
                bathrooms = re.findall(r'\d+', bathrooms_text)[0]
            rent_string = ''.join(response.xpath('////p[@class="price"]/text()').extract())
            descrription = ''.join(response.xpath('//div[contains(@class,"full_description")]//text()').extract())
            summary = ''.join(response.xpath('//p[@class="main_summary"]//text()').extract()) 
            full_description = descrription + summary   
            item_loader = ListingLoader(response=response)
            item_loader.add_value('property_type', property_type)
            item_loader.add_xpath('latitude', "substring-before(//div[@class='google-map-embed']/@data-location,',')")
            item_loader.add_xpath('longitude', "substring-after(//div[@class='google-map-embed']/@data-location,',')")
            item_loader.add_value('external_link', external_link)
            externalid=external_link.split("/")[-1]
            if externalid:
                item_loader.add_value("external_id",externalid)
            dontallow=response.xpath("//div[@class='ribbon base']//span/text()").get()
            if dontallow and "let agreed" in dontallow.lower():
                return 
            

            item_loader.add_value('title', title)
            item_loader.add_value('address', address)
            item_loader.add_value('city', item_loader.get_collected_values("address")[0].split(',')[-1].strip())
            item_loader.add_value('description', full_description)
            item_loader.add_value('rent_string', rent_string)
            item_loader.add_xpath('images', '//a[contains(@class, "fancybox-thumbs")]/@href')
            item_loader.add_value('room_count', str(room_count))
            item_loader.add_value('bathroom_count', str(bathrooms))
            furnished=response.xpath("//h4[.='Key Info']/following-sibling::ul//li/text()").getall()
            if furnished:
                for i in furnished:
                    if "furnished" in i.lower():
                        item_loader.add_value("furnished",True)
            item_loader.add_value('landlord_name', 'Maguire Jackson')
            item_loader.add_value('landlord_email', 'bham@maguirejackson.com')
            item_loader.add_value('landlord_phone', '0121 634 1520')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            yield item_loader.load_item()