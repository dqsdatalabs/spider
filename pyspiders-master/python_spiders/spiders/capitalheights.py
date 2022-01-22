# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
import re
from ..loaders import ListingLoader 

class CapitalheightsSpider(scrapy.Spider):
    name = "capitalheights"
    allowed_domains = ["www.capitalheights.co.uk"] 
    start_urls = (
        'http://www.www.capitalheights.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        for page in range(1, 20):
            start_urls = 'https://www.capitalheights.co.uk/let/property-to-let//page/{}'.format(page)
            yield scrapy.Request( url=start_urls, callback=self.parse, dont_filter=True)

    def parse(self, response, **kwargs):
        links = response.xpath('//div[@class="module-content"]/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True)

    def get_property_details(self,response):
        if 'flat' in response.url:
            property_type = 'apartment'
        elif 'house' in response.url: 
            property_type = 'house'
        else:
            property_type = '' 
        if property_type:
            item_loader = ListingLoader(response=response)
            external_link = response.url
            externalid=response.url
            if externalid:
                externalid=externalid.split("property/")[-1].split("/")[0]
                item_loader.add_value("external_id",externalid)
            title= "".join(response.xpath("//h1/text()").getall())
            if title:
                item_loader.add_value("title",title.strip())
            address = ''.join(response.xpath('//div[@id="pdetails"]//h1/text()').extract()).strip()   
            city = address.split(', ')[-2]
            zipcode = address.split(', ')[-1] 
            room_count_text = response.xpath('//div[@id="pdetails"]//span[contains(text(), "BEDROOM")]/text()').extract_first('').strip()
            if room_count_text:
                room_count = re.findall(r'\d+', room_count_text)[0]
            else:
                room_count = ''
            bathroom_text = response.xpath('//div[@id="pdetails"]//span[contains(text(), "BATHROOM")]/text()').extract_first('').strip() 
            if bathroom_text:
               bathrooms = re.findall(r'\d+', bathroom_text)[0]
            else:
                bathrooms = ''
            rent_string = response.xpath('//div[@id="pdetails"]//span[@class="nativecurrencyvalue"]/text()').extract_first('').strip()
            rent_value = re.findall(r'([\d|,|\.]+)', rent_string)[0].replace(',', '')
            rent_month = str(int(rent_value) * 4) + '£'
            floor_plan_images = response.urljoin(response.xpath('//a[@class="floorplan-button"]/@href').extract_first())
            lat_lon = re.search(r'initStreetView\((.*?)\)', response.text)
            if lat_lon:
                lat_lon = lat_lon.group(1)
                latitude = lat_lon.split(',')[0]
                longitude = lat_lon.split(',')[1]  
                item_loader.add_value('latitude', str(latitude))
                item_loader.add_value('longitude', str(longitude))
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('address', address)
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_xpath('description', '//h3[contains(text(), "Description")]/following-sibling::p//text()')
            item_loader.add_value('rent_string', rent_month)
            item_loader.add_xpath('images', '//div[@class="item lazyload"]/@data-image-src')
            
            item_loader.add_value('floor_plan_images', floor_plan_images)
            item_loader.add_value('room_count', str(room_count))
            item_loader.add_value('bathroom_count', str(bathrooms))
            item_loader.add_value('landlord_name', 'Capital Heights')
            item_loader.add_value('landlord_email', 'info@capitalheights.co.uk')
            item_loader.add_value('landlord_phone', '0207 078 0077')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            yield item_loader.load_item()
