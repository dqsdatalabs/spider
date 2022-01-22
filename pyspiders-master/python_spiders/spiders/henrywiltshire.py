# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
import re
from ..helper import format_date, remove_white_spaces
import dateparser


class HenrywiltshireSpider(scrapy.Spider):
    name = "henrywiltshire" 
    allowed_domains = ["henrywiltshire.co.uk"]
    start_urls = (
        'http://www.henrywiltshire.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    external_source = "Henrywiltshire_PySpider_united_kingdom_en"
    
    def start_requests(self):
        start_urls = [
            {'url': 'https://www.henrywiltshire.co.uk/property-for-rent/united-kingdom/?property_type=Apartment&view=1&location=', 'property_type': 'apartment'},
            {'url': 'https://www.henrywiltshire.co.uk/property-for-rent/united-kingdom/?property_type=Flat&view=1&location=', 'property_type': 'apartment'},
            {'url': 'https://www.henrywiltshire.co.uk/property-for-rent/united-kingdom/?property_type=House&view=1&location=', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )
    def parse(self, response, **kwargs):
        links = response.xpath('//div[@id="search-result-grid"]//div[@class="property-box"]/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, meta={'property_type': response.meta.get('property_type')})
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        external_link = response.url
        external_id = response.xpath('//p[contains(text(), "Ref")]/text()').extract_first('').split(': ')[-1]
        title = response.xpath('//title/text()').extract_first()    
        if 'house' in external_link.lower():
            property_type = 'house'
        else:
            property_type = response.meta.get('property_type')
        address = title.split(':')[-1]
        try: 
            #city = address.split(', ')[-2].strip()
            city = 'London'
        except:
            city = ''
        try:
            zipcode = address.split(', ')[-1]   
        except:
            zipcode = ''
        
        zipcode = response.xpath("//h1/text()").get()
        if zipcode:
            zipcode = zipcode.split(",")[-1].strip().split(" ")[-1]
            if zipcode and not zipcode.isalpha():
                zipcode1= zipcode.split(",")[-1].strip().split(" ")[-1]
                # item_loader.add_value('zipcode', zipcode1)
                # item_loader.add_value("zipcode", zipcode.split(",")[-1].strip().split(" ")[-1])
            else:
                zipcode1 = ''
                # item_loader.add_value('zipcode', zipcode1)

        

        try:
            room_count = int(response.xpath('//li[@class="bedrooms"]/span//text()').extract_first('').strip())
        except:
            room_count = ''
        bathrooms = response.xpath('//li[@class="bathrooms"]/span//text()').extract_first('').strip()
        bathrooms = str(bathrooms)
        rent_string = response.xpath('//section[contains(@class, "property-features")]/h3/text()').extract_first('').strip()
        rent_month = ""
        if "month" not in rent_string:
            rent_value = re.findall(r'([\d|,|\.]+)', rent_string)[0].replace(',', '')
            rent_month = str(float(rent_value) * 4) + '£'
        else:
            rent_value = rent_string.split(" ")[0].replace(",","").replace("£","")
            rent_month = rent_value
        square_meters_text = response.xpath('//li[@class="area"]/span//text()').extract_first()
        try:
            square_meters_ft = re.findall(r'\d+', square_meters_text)[0]
            square_meters = str(int(int(square_meters_ft) / 10.764))
        except:
            square_meters = ''
        furnished = ''
        descriptions = ''.join(response.xpath('//section[@class="propertydesc"]//text()').extract())
        balcony = ''
        floor = ''
        if 'balcony' in descriptions.lower():
            balcony = True 
        parking = ''
        if 'park' in descriptions.lower():
            parking = True 
        terrace = ''
        if 'terrace' in descriptions.lower():
            terrace = True 
        if 'floor' in descriptions.lower():
            try:
                floor = re.search(r'the\s(\d+)\w+\sfloor', descriptions).group(1)
            except:
                floor = ''
        features = response.xpath('//section[contains(@class, "property-features")]//text()').extract()
        for feature in features:
            if 'furnished' in feature.lower():
                furnished = True  
        available_date_text = response.xpath('//section[contains(@class, "property-features")]//p[contains(text(), "Available")]/text()').extract_first('').strip()
        if available_date_text:
            date_parsed = dateparser.parse( remove_white_spaces(available_date_text.split(' ')[-1]), date_formats=["%d %B %Y"] )
            available_date = date_parsed.strftime("%Y-%m-%d")
        else:
            available_date = ''
        imglist = []
        images = response.xpath('//div[@id="propertypage-slider"]//div[@class="rsBgimg"]')
        for img in images:
            img_url = img.xpath('./@style').extract_first()
            img_href = re.search(r'url\((.*?)\)', img_url).group(1)
            imglist.append(img_href) 
        lat = response.xpath('//div[@class="marker"]/@data-lat').extract_first('').strip()
        lon = response.xpath('//div[@class="marker"]/@data-lng').extract_first('').strip() 
        floor_plan_img = response.xpath('//h2[contains(text(), "Floor Plans")]/following-sibling::div/a/@href').extract()
        if room_count: 
 
            item_loader.add_value('external_id', external_id)
            item_loader.add_value('property_type', property_type) 
            item_loader.add_value('external_link', external_link) 
            item_loader.add_value('title', title)
            item_loader.add_value('address', address)
            item_loader.add_xpath('description', '//section[@class="propertydesc"]//text()')
            item_loader.add_value('rent', int(float(rent_month.replace("£",""))))
            item_loader.add_value("currency", "GBP")
            item_loader.add_value('images', imglist) 
            if len(floor_plan_img) > 0:
                item_loader.add_value('floor_plan_images', floor_plan_img)  
            if square_meters:
                item_loader.add_value('square_meters', square_meters)
            if floor:
                item_loader.add_value('floor', str(floor))
            if lat:
                item_loader.add_value('latitude', str(lat))
            if lon:
                item_loader.add_value('longitude', str(lon))
            if city:
                item_loader.add_value('city', city) 
            if furnished:
                item_loader.add_value('furnished', True)
            if zipcode1:
                item_loader.add_value('zipcode', zipcode1)
            if balcony:
                item_loader.add_value('balcony', True)
            if parking:
                item_loader.add_value('parking', True)
            if terrace:
                item_loader.add_value('terrace', True)
            if available_date:
                item_loader.add_value('available_date', available_date)
            item_loader.add_value('room_count', str(room_count))
            item_loader.add_value('bathroom_count', bathrooms)
            item_loader.add_value('landlord_name', 'Henry Wiltshire International')
            item_loader.add_value('landlord_email', 'canarywharf@henrywiltshire.com')
            item_loader.add_value('landlord_phone', '+44 (0)20 7001 9160')
            # item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            item_loader.add_value("external_source", self.external_source)
            
            status = response.xpath("//div[contains(@class,'propsold')]/text()").get()
            if not status:
                yield item_loader.load_item()