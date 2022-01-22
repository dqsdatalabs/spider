# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader

def cleantext(text):
    p_text = re.findall(r'([\d|,|\.]+)', text, re.S | re.M | re.I)[0]
    exce_p = p_text.replace(',', '').replace('.00', '')
    return exce_p

class DavidshomesSpider(scrapy.Spider):
    name = "davidshomes"
    allowed_domains = ["www.davidshomes.co.uk"]
    start_urls = (
        'http://www.www.davidshomes.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    external_source='Davidshomes_PySpider_united_kingdom_en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        url = 'https://www.davidshomes.co.uk/properties.asp?page=1&pageSize=50&orderBy=PriceSearchAmount&orderDirection=DESC&PropInd=L&businessCategoryId=1'
        yield scrapy.Request(url=url, callback=self.parse, dont_filter=True)

    def parse(self, response, **kwargs):
        pages = response.xpath('//div[@class="searchprop"]/following-sibling::div[@class="paging"]//span[@class="pagersummary"]/text()').extract_first().split(' of ')[-1]
        for page in range(1, int(pages)+1):
            link = 'https://www.davidshomes.co.uk/properties.asp?page={}&pageSize=50&orderBy=PriceSearchAmount&orderDirection=DESC&PropInd=L&businessCategoryId=1'.format(page)
            yield scrapy.Request(url=link, callback=self.get_sub_urls, dont_filter=True)

    def get_sub_urls(self, response):
        links = response.xpath('//div[@class="searchprop"]//div[@class="address"]/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True)
    
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        property_type_site = ''.join(response.xpath('//div[@class="twocolfeaturelistcol1"]//li//text()').extract())
        description = ''.join(response.xpath('//div[@class="description"]//text()').extract())
        if 'apartment' in description.lower() or 'apartment' in property_type_site.lower():
            property_type = 'apartment'
        elif 'house' in description.lower() or 'house' in property_type_site.lower():
            property_type = 'house'
        elif 'home' in property_type_site.lower():
             property_type = 'house'
        elif 'flat' in description.lower():
            property_type = 'apartment' 
        else:
            property_type = ''
        if property_type:
            external_link = response.url
            address = response.xpath('//div[@class="headline"]/text()').extract_first('').strip()
            city_zipcode = address.split(', ')[-1]
            city = city_zipcode.split(' ')[0]
            zipcode = city_zipcode.replace(city, '').replace(".","").strip()
            room_count_text = response.xpath('//div[@class="beds"]/text()').extract_first('').strip()
            if room_count_text:
                try:
                    room_count = re.findall(r'\d+', room_count_text)[0]     
                except:
                    room_count = ''
            
            rent_string = response.xpath('//span[@class="displayprice"]/text()').extract_first('').strip()
            try:
                lat_lon = re.search(r'javascript:loadGoogleMapv3\((.*?)\)', response.text).group(1)
                latitude = lat_lon.split(',')[0] 
                longitude = lat_lon.split(',')[1]  
            except:
                latitude = ''
                longitude = ''
            price_counts = "".join(response.xpath("//div[@class='propertyroomcontent'][contains(.,'Deposit') or contains(.,'DEPOSIT')]/div[contains(@class,'desc')]/text()").extract())
            deposit = ""
            if price_counts:
                if "deposit" in price_counts.lower() and ":" in price_counts:
                    deposit = price_counts.split(":")[-1].split(".")[0].replace("£","").strip()
                elif "£" in price_counts:
                    deposit = price_counts.split("£")[1].split(".")[0].replace(",","")
            
            if deposit:
                item_loader.add_value("deposit", deposit)
                
            contents = response.xpath("//div[@class='features']//div/ul/li[contains(.,'Furnished')]/text()").extract_first() 

            if room_count and latitude and longitude: 
                
                item_loader.add_value('property_type', property_type)
                item_loader.add_value('external_link', external_link)
                title = response.xpath("//title//text()").get()
                if title:
                    title = re.sub('\s{2,}', ' ', title.strip())
                    item_loader.add_value("title", title)
                item_loader.add_value('address', address)
                item_loader.add_value('city', city)
                if zipcode.count(" ")==1:
                    item_loader.add_value("zipcode", zipcode)
                elif zipcode.count(" ")>1: 
                    item_loader.add_value('zipcode', f"{zipcode.split(' ')[-2]} {zipcode.split(' ')[-1]}")

                if contents:
                    item_loader.add_value('furnished', True)

                item_loader.add_xpath('description', '//div[@class="description"]//text()')
                item_loader.add_value('rent_string', rent_string)
                item_loader.add_xpath('images', '//img[@class="propertyimage"]/@src')
                if latitude:
                    item_loader.add_value('latitude', str(latitude))
                if longitude:
                    item_loader.add_value('longitude', str(longitude))
                item_loader.add_value('room_count', str(room_count))
                
                parking = response.xpath("//li[contains(.,'Parking')]").get()
                if parking:
                    item_loader.add_value("parking", True)
                
                item_loader.add_value('landlord_name', 'Davids homes')
                item_loader.add_value('landlord_email', 'info@davidshomes.co.uk')
                item_loader.add_value('landlord_phone', '02920 30 30 30')
                item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
                yield item_loader.load_item() 