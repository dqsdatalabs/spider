# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re

from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import extract_number_only, remove_white_spaces, extract_rent_currency
import js2xml
import lxml
from scrapy import Selector


class JamesoliverComSpider(scrapy.Spider):
    name = "jamesoliver_com"
    allowed_domains = ["jamesoliver.com"]
    start_urls = ['https://jamesoliver.com/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    external_source='Jamesoliver_PySpider_united_kingdom_en'
    custom_settings = {
        "PROXY_TR_ON":True,
        "RETRY_HTTP_CODES": [500, 503, 504, 400, 401, 403, 405, 407, 408, 416, 456, 502, 429, 307],
        "HTTPCACHE_ENABLED": False
    }

    def start_requests(self):
        start_urls = ['https://jamesoliver.com/properties/']
        headers = {
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
        }
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 headers=headers,
                                 meta={'request_url':url})

    def parse(self, response, **kwargs):
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36"
        }
        
        
        
        page = response.meta.get('page', 2)
        seen = False
        listings = response.xpath('//a[contains(text(),"More Details")]/@href').extract()
        for property_url in listings:
            property_url = response.urljoin(property_url)
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                headers=headers,
                meta={'request_url': property_url}
            )
            seen = True
        
        if page ==2 or seen:
            f_url = f"https://jamesoliver.com/properties/page/{page}/"
            yield scrapy.Request(f_url, callback=self.parse, headers=headers, meta={"page": page+1, 'request_url': property_url})
                      
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        title = response.xpath('.//meta[@property="og:title"]/@content').extract_first()
        if title:
            title_str = re.sub(('-|–|—'),'-',title).split('-')[:-1]
            title_string = ', '.join([remove_white_spaces(y) for y in title_str if len(y)>1])
            item_loader.add_value('title',title_string)
        item_loader.add_xpath('description','.//*[@class="summary-contents"]//text()')
        description = item_loader.get_output_value('description')
        item_loader.add_xpath('address','.//p[@class="elementor-heading-title elementor-size-default"]/text()')
        address = item_loader.get_output_value('address')
        if address:
            zipcode1 = re.search(r'(GIR|[A-Za-z]\d[A-Za-z\d]?|[A-Za-z]{2}\d[A-Za-z\d]?)[ ]??(\d[A-Za-z]{0,2})??$',address,re.IGNORECASE)
            zipcode2 = re.search(r'(([A-Z][A-HJ-Y]?\d[A-Z\d]?|ASCN|STHL|TDCU|BBND|[BFS]IQQ|PCRN|TKCA) ?\d[A-Z]{2}|BFPO ?\d{1,4}|(KY\d|MSR|VG|AI)[ -]?\d{4}|[A-Z]{2} ?\d{2}|GE ?CX|GIR ?0A{2}|SAN ?TA1)$',address,re.IGNORECASE)
            if zipcode1:
                item_loader.add_value('zipcode',remove_white_spaces(zipcode1.group()))
                city = remove_white_spaces(address.rstrip(zipcode1.group())).rstrip(',')
                city = remove_white_spaces(city.split(',')[-1])
                item_loader.add_value('city',city)
            elif zipcode2:
                item_loader.add_value('zipcode',remove_white_spaces(zipcode2.group()))
                city = remove_white_spaces(address.rstrip(zipcode2.group())).rstrip(',')
                city=remove_white_spaces(city.split(',')[-1])
                item_loader.add_value('city',city)
            elif zipcode1==None and zipcode2==None and len(address.split(', '))>1:
                if ''.join(remove_white_spaces(address.split(',')[-1]).split()).isalpha()==False:
                    city_zip = remove_white_spaces(address.split(',')[-1])
                    city_zip = re.sub(('-|–|—'),' ',city_zip)
                    if re.findall(r'\s',remove_white_spaces(city_zip)).count(' ')==0:
                        item_loader.add_value('zipcode',city_zip)
                        item_loader.add_value('city',remove_white_spaces(address.split(',')[-2]))
                    elif re.findall(r'\s',remove_white_spaces(city_zip)).count(' ')>0:
                        zipp = remove_white_spaces(city_zip).split(' ')[:-1]
                        if ''.join(zipp).isalpha()==True:
                            item_loader.add_value('city',' '.join(zipp))
                            item_loader.add_value('zipcode',city_zip.split(' ')[-1])
                        elif ''.join(zipp).isalpha()==False:
                            item_loader.add_value('zipcode',' '.join(city_zip.split(' ')[-2:]))
                            zipps = item_loader.get_output_value('zipcode')
                            item_loader.add_value('city',remove_white_spaces(city_zip.rstrip(zipps)))
                else:
                    city = remove_white_spaces(address.split(',')[-1])
                    item_loader.add_value('city',city.strip(','))
            else:
                city = remove_white_spaces(address.split(',')[-1])
                item_loader.add_value('city',city.strip(','))
                
        apartment_types = ["appartement", "apartment", "flat","penthouse", "duplex", "triplex"]
        house_types = ['chalet', 'bungalow', 'maison', 'house', 'home', 'villa']
        studio_types = ["studio"]
        if any (i in title.lower() for i in studio_types) or any(i in response.meta.get("request_url").lower() for i in studio_types) or any(i in description.lower() for i in studio_types):
            item_loader.add_value('property_type', "studio")
        elif any (i in title.lower() for i in house_types) or any(i in response.meta.get("request_url").lower() for i in house_types) or any(i in description.lower() for i in house_types):
            item_loader.add_value('property_type', "house")
        elif any (i in title.lower() for i in apartment_types) or any(i in response.meta.get("request_url").lower() for i in apartment_types) or any(i in description.lower() for i in apartment_types):
            item_loader.add_value('property_type', 'apartment')
        
        property_type = item_loader.get_output_value('property_type')
                
        bathroom_count = response.xpath('//*[@class="elementor-widget-bathrooms"]/text()').extract_first()
        if bathroom_count and str(extract_number_only(bathroom_count))!='0':
            item_loader.add_value('bathroom_count',str(extract_number_only(bathroom_count)))
            
        room_count = response.xpath('//*[@class="elementor-widget-bedrooms"]/text()').extract_first()
        if room_count and str(extract_number_only(bathroom_count))=='0' and property_type=="studio":
            item_loader.add_value('room_count','1')
        elif room_count==None and property_type=="studio":
            item_loader.add_value('room_count','1')
        elif room_count and str(extract_number_only(bathroom_count))!='0':
            item_loader.add_value('room_count',str(extract_number_only(room_count)))
        
        # rent_string = response.xpath('.//*[@class="price"]//text()').extract()
        # if rent_string:
        #     rent_string = remove_white_spaces(' '.join([remove_white_spaces(y) for y in rent_string if len(y)>1]))
        #     if any(word in rent_string.lower() for word in ['week','pw','pppw','pcw']):
        #         rent = extract_rent_currency(rent_string,JamesoliverComSpider)[0]*4
        #         item_loader.add_value('rent_string','£'+str(int(rent)))
        #     elif any(word in rent_string.lower() for word in ['month','pm','pcm']):
        #         item_loader.add_value('rent_string',rent_string)
        #     else:
        #         item_loader.add_value('rent_string',rent_string)
        
        images = response.xpath('.//*[@class="propertyhive-main-image"]/@href').extract()
        if images:
            item_loader.add_value('images',list(set(images)))
            
        features_list = response.xpath('.//*[@class="features"]//li/text()').extract()
        if features_list:
            features = ', '.join(features_list).lower()
            
            if re.search(r'un[^\w]*furnish',features):
                item_loader.add_value('furnished',False)
            elif re.search(r'not[^\w]*furnish',features):
                item_loader.add_value('furnished',False)
            elif re.search(r'furnish',features):
                item_loader.add_value('furnished',True)
            if re.search(r'dish[^\w]*washer',features):
                item_loader.add_value('dishwasher',True)
            if re.search(r'swimming[^\w]*pool',features) or 'pool' in features:
                item_loader.add_value('swimming_pool',True)
            if 'terrace' in features:
                item_loader.add_value('terrace',True)
            if 'balcony' in features:
                item_loader.add_value('balcony',True)
            if 'parking' in features:
                item_loader.add_value('parking',True)
            if 'lift' in features or 'elevator' in features:
                item_loader.add_value('elevator',True)
            if re.search(r'no[^\w]*pet\w{0,1}[^\w]*allow',features):
                item_loader.add_value('pets_allowed',False)
            elif re.search(r'pet\w{0,1}[^\w]*not[^\w]*allow',features):
                item_loader.add_value('pets_allowed',False)
            elif re.search(r'pet\w{0,1}[^\w]*allow',features):
                item_loader.add_value('pets_allowed',True)
                
        javascript = response.xpath('.//script[contains(text(),"LatLng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            lat_lng = selector.xpath('.//identifier[@name="LatLng"]/../../..//number/@value').extract()
            if lat_lng:
                item_loader.add_value('latitude', lat_lng[0])
                item_loader.add_value('longitude', lat_lng[1])
                
        item_loader.add_value('landlord_name', "James Oliver")
        item_loader.add_value('landlord_phone', '023 9281 6347')
        item_loader.add_value('landlord_email', 'info@jamesoliver.com')
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.meta.get("request_url"))

        self.position += 1
        item_loader.add_value("position", self.position)

        if item_loader.get_collected_values("property_type"):
            yield item_loader.load_item()
